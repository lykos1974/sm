# ROS Knowledge Migration Review

## Scope

This review evaluates whether the current Research Operating System (ROS) architecture, defined by `RESEARCH_OPERATING_SYSTEM_BLUEPRINT.md` and `RESEARCH_OPERATING_SYSTEM_ADVANCED.md`, can represent existing historical PnF project knowledge without inventing ad-hoc structures.

This is a knowledge migration and architecture review only. It does not design a detector, implement code, create datasets, run research, backtest, optimize, or modify any existing detector or live execution workflow.

## Review Standard

For each historical research family, this review asks:

- Which ROS objects represent it?
- Which registry owns it?
- Which metadata fields are missing?
- Which relationships are missing?
- Which promotion stage it belongs to?
- Whether traceability is preserved?

A missing capability is only listed when real historical project knowledge cannot be represented cleanly by the current ROS documents.

## Executive Assessment

The current ROS is broadly sufficient to preserve the known project history. The core object model already covers hypotheses, features, edges, failures, validation runs, reports, promotion decisions, research debt, rollback points, confidence history, live shadow concepts, and experiment-level reproducibility.

The main gaps are minor and operational rather than structural:

1. MEXC live execution workflow knowledge would benefit from a formal `ExecutionWorkflow` or `Runbook` record type, or an explicit runbook field under `LiveDeployment`.
2. Forward validation and live-shadow observations would benefit from a clearer `OBSERVED_IN` or `FORWARD_OBSERVES` relationship, although they can already be represented through `ValidationRun`, `LiveDeployment`, `Report`, and `REFERENCES`.
3. Rollback points are present in the proposed folder structure and governance, but the advanced graph does not define `RollbackPoint` as a node type; it can currently be represented as `PromotionDecision` metadata, but a first-class node would improve traceability.

These are minor gaps. They do not require redesigning the ROS.

## Historical Research Family Mapping

### 1. Pole Research

| Existing Knowledge | Current ROS Representation | Missing Capability | Recommendation |
| --- | --- | --- | --- |
| Pole setup idea and variants | `Hypothesis`, `Experiment`, `Feature`, `ValidationRun`, `Report` | No required missing capability. | Migrate each serious Pole idea or variant as a `Hypothesis` linked to universal experiment IDs. |
| Pole-derived reusable conditions such as breakout context, pullback quality, active leg boxes, extension state, and direction filters | `Feature` records in `feature_store/` with feature manifests; graph `Feature DEPENDS_ON Dataset` and `Edge DEPENDS_ON Feature` | No required missing capability. | Register stable Pole labels and filters as versioned features rather than embedding them inside experiment notes. |
| Validated Pole behavior | `Edge` in `edge_registry/validated/` or `edge_registry/promoted/` | No required missing capability. | Store validated Pole evidence as one or more `Edge` records with population, expectancy, robustness, and known weaknesses. |
| Pole reports and scorecards | `Report`, `ValidationRun`, artifact manifests | No required missing capability. | Link reports to validation runs using `Report REFERENCES ValidationRun`. |
| Pole failures or discarded variants | `Failure` in `failure_registry/` | No required missing capability. | Preserve rejected Pole variants with `Failure REJECTS Hypothesis` and `do_not_repeat_notes`. |

- **Owning registries**: `hypothesis_registry`, `feature_store`, `edge_registry`, `failure_registry`, `validation_engine/run_manifests`, `reports`, `promotion_engine`.
- **Promotion stage**: Ranges from `IDEA` through `PRODUCTION`; the known profitable Pole baseline belongs at least to `PRODUCTION` or promoted baseline status, while variants may be earlier stages.
- **Traceability**: Preserved through `Experiment`, `ValidationRun`, `Edge`, `Report`, `PromotionDecision`, and graph relationships.

### 2. SL-C Motif Research

| Existing Knowledge | Current ROS Representation | Missing Capability | Recommendation |
| --- | --- | --- | --- |
| SL-C motif as a research concept | `Hypothesis` | No required missing capability. | Register each motif claim as a hypothesis with expected mechanism and population. |
| SL-C motif detection prerequisites | `Feature` manifests and dependency graph | No required missing capability. | Represent motif labels and upstream structure as features, even if the motif was ultimately rejected. |
| SL-C validation or exploratory findings | `Experiment`, `ValidationRun`, `Report` | No required missing capability. | Assign experiment IDs to serious historical SL-C tests and link reports. |
| Negative or inconclusive SL-C results | `Failure` or `INCONCLUSIVE` hypothesis status | No required missing capability. | Store rejected or inconclusive findings in the failure registry with revisit conditions. |
| SL-C lessons for future research | `Failure`, `ResearchDebtItem`, `Meta-research` outputs | No required missing capability. | Preserve repeated blockers or unresolved proof requirements as research debt only if they still matter. |

- **Owning registries**: `hypothesis_registry`, `feature_store`, `failure_registry`, `validation_engine/run_manifests`, `reports`.
- **Promotion stage**: Usually `HYPOTHESIS_DEFINED`, `POPULATION_VALIDATED`, `EXPECTANCY_VALIDATED`, `FAILED`, or `INCONCLUSIVE`, depending on historical evidence.
- **Traceability**: Preserved if every serious SL-C test receives an `Experiment` ID and failures link back to hypotheses and reports.

### 3. Next Open Entry Findings

| Existing Knowledge | Current ROS Representation | Missing Capability | Recommendation |
| --- | --- | --- | --- |
| Entry timing observations around next open behavior | `Hypothesis`, `Feature`, `ValidationRun`, `Report` | No required missing capability. | Represent entry timing as hypotheses and entry-time features, not as informal notes. |
| Findings that alter expected execution assumptions | `ResearchDebtItem`, `ValidationRun`, `PromotionDecision` evidence | Potential minor metadata gap: execution assumption fields are not explicitly named in validation or promotion contracts. | Add execution-assumption metadata to validation run manifests when implemented; no structural redesign required. |
| Entry findings that affect live eligibility | `PromotionDecision`, `ProductionCandidate`, `LiveDeployment` | No required missing capability. | Link entry findings to promotion gates as evidence or blockers. |
| Rejected entry ideas | `Failure` | No required missing capability. | Use failure records with `failed_stage`, `key_metrics`, and `do_not_repeat_notes`. |

- **Owning registries**: `hypothesis_registry`, `feature_store`, `validation_engine/run_manifests`, `failure_registry`, `promotion_engine`, `reports`.
- **Promotion stage**: Typically `EXPECTANCY_VALIDATED`, `STABILITY_VALIDATED`, `FORWARD_VALIDATED`, or a blocker before `LIVE_SHADOW`.
- **Traceability**: Preserved, provided execution assumptions are captured in validation or promotion metadata.

### 4. Harmonic Research

| Existing Knowledge | Current ROS Representation | Missing Capability | Recommendation |
| --- | --- | --- | --- |
| Harmonic detector concepts and variants | `Hypothesis`, `Experiment`, `Feature` | No required missing capability. | Map each serious harmonic pattern or filter as a hypothesis with required features. |
| Harmonic structural labels | `Feature` records and graph dependencies | No required missing capability. | Store pattern labels, ratios, pivots, and structure primitives as versioned features. |
| Harmonic validation history | `ValidationRun`, `Report`, `Experiment` | No required missing capability. | Preserve historical run manifests and reports under experiment IDs. |
| Harmonic validated edges, if any | `Edge` | No required missing capability. | Promote only validated harmonic behavior into edge records; otherwise keep as hypothesis/failure. |
| Harmonic failed ideas | `Failure` | No required missing capability. | Record failure reason, evidence, population, key metrics, and revisit conditions. |

- **Owning registries**: `hypothesis_registry`, `feature_store`, `edge_registry` when validated, `failure_registry`, `validation_engine/run_manifests`, `reports`.
- **Promotion stage**: Depends on evidence; likely ranges from `HYPOTHESIS_DEFINED` to `STABILITY_VALIDATED`, `FAILED`, or `INCONCLUSIVE`.
- **Traceability**: Preserved through experiment IDs, feature dependencies, validation records, and reports.

### 5. ABCD Research

| Existing Knowledge | Current ROS Representation | Missing Capability | Recommendation |
| --- | --- | --- | --- |
| ABCD pattern hypotheses | `Hypothesis` | No required missing capability. | Register each ABCD mechanism and variant as a hypothesis. |
| ABCD structural components | `Feature` manifests | No required missing capability. | Treat pivots, legs, ratios, completion zones, and confirmation labels as features. |
| ABCD validation outputs | `ValidationRun`, `Report`, `Experiment` | No required missing capability. | Link historical ABCD reports to run manifests and experiment IDs. |
| ABCD edge candidates | `Edge` or `ProductionCandidate` if validated | No required missing capability. | Only migrate supported ABCD behavior as an edge; otherwise keep as hypothesis or failure. |
| ABCD negative results | `Failure` | No required missing capability. | Preserve failures and do-not-repeat notes. |

- **Owning registries**: `hypothesis_registry`, `feature_store`, `edge_registry`, `failure_registry`, `validation_engine/run_manifests`, `reports`.
- **Promotion stage**: `HYPOTHESIS_DEFINED` through `EXPECTANCY_VALIDATED`, `FAILED`, or `INCONCLUSIVE` depending on historical evidence.
- **Traceability**: Preserved by the current ROS.

### 6. Existing Profitable Pole Baseline

| Existing Knowledge | Current ROS Representation | Missing Capability | Recommendation |
| --- | --- | --- | --- |
| Stable rollback profile: long only, post-breakout pullback, healthy pullback, two active leg boxes, non-extended, all symbols in scope, no early breakeven | `Edge`, `PromotionDecision`, `ProductionCandidate`, `RollbackPoint` concept, `Feature` dependencies | Minor gap: `RollbackPoint` is in the Blueprint folder structure and governance, but not a first-class advanced graph node. | Keep baseline as a promoted `Edge` plus `PromotionDecision`; add first-class rollback metadata or node when migration tooling begins. |
| Baseline metrics such as candidate rows, resolved rows, win rate, average R, total R, and TP1 to TP2 conversion | `Edge.expectancy`, `Edge.population`, `ValidationRun`, `Report` | No required missing capability. | Store baseline scorecard in edge metadata and source validation run. |
| Baseline as comparison standard | Time-saving rules, validation engine comparison reports, promotion governance | No required missing capability. | Link every serious competing experiment to the baseline edge via `REFERENCES` or comparison report. |
| Baseline as rollback point for future changes | `promotion_engine/rollback_points/`, `PromotionDecision`, governance | Minor gap noted above. | Make rollback evidence explicit during migration; no ROS redesign required. |

- **Owning registries**: `edge_registry/promoted`, `promotion_engine/decisions`, `promotion_engine/rollback_points`, `validation_engine/run_manifests`, `reports`, `feature_store`.
- **Promotion stage**: `PRODUCTION` or stable promoted baseline.
- **Traceability**: Mostly preserved; strongest if rollback point becomes either a first-class node or mandatory promotion metadata.

### 7. MEXC Live Execution Workflow

| Existing Knowledge | Current ROS Representation | Missing Capability | Recommendation |
| --- | --- | --- | --- |
| MEXC live trader workflow and operational rules | `LiveDeployment`, `ProductionCandidate`, `PromotionDecision`, `adapters/mexc_shadow/`, `Report` | Genuine minor gap: current ROS lacks an explicit `ExecutionWorkflow` or `Runbook` object/contract. | Do not redesign; add a future runbook record type or required `runbook_reference`, execution assumptions, and operational controls under `LiveDeployment`. |
| Live-shadow workflow | `LIVE_SHADOW` promotion stage, `LiveDeployment`, `Report` | Minor relationship gap: no explicit `OBSERVED_IN` or `SHADOW_OBSERVED` relationship. | Existing `REFERENCES` and `GENERATED_FROM` are sufficient for migration; consider adding a clearer observation relationship later. |
| Micro-live or production deployment records | `LiveDeployment`, `PromotionDecision`, `ProductionCandidate` | No required missing capability. | Migrate deployment records as `LiveDeployment` nodes linked to promotion decisions. |
| Execution model assumptions, slippage, latency, order behavior | `ValidationRun` policy versions, `PromotionDecision`, `ResearchDebtItem` | Minor metadata gap: operational execution assumptions are not fully enumerated. | Add execution assumptions to validation and live deployment manifests during implementation. |

- **Owning registries**: `promotion_engine`, `adapters`, `reports`, advanced graph `LiveDeployment`, future live deployment records.
- **Promotion stage**: `LIVE_SHADOW`, `MICRO_LIVE`, or `PRODUCTION`, depending on historical state.
- **Traceability**: Preserved for promotion and deployment events; operational workflow details need a runbook reference to avoid relying on free-form reports.

### 8. Validation History

| Existing Knowledge | Current ROS Representation | Missing Capability | Recommendation |
| --- | --- | --- | --- |
| Historical validation runs | `ValidationRun`, `Experiment`, `Report`, run manifests | No required missing capability. | Migrate every serious run into a validation run manifest, even if incomplete. |
| Cost, slippage, ambiguity, symbol/year splits, walk-forward logic | Shared Validation Engine policy versions and run manifests | No required missing capability. | Attach policy version fields to each migrated run. |
| Output metrics | Validation run metrics, reports, edge records | No required missing capability. | Preserve required scorecards and link to reports. |
| Incomplete legacy validation metadata | `ResearchDebtItem` or report notes | No required missing capability. | Mark incomplete metadata as research debt or migration caveat rather than inventing structures. |

- **Owning registries**: `validation_engine/run_manifests`, `reports`, `manifests/runs`, advanced graph `ValidationRun`.
- **Promotion stage**: Supports all stages from `POPULATION_VALIDATED` through `FORWARD_VALIDATED` and beyond.
- **Traceability**: Preserved if run manifests link to dataset manifests, feature versions, policy versions, reports, and experiment IDs.

### 9. Forward Validation

| Existing Knowledge | Current ROS Representation | Missing Capability | Recommendation |
| --- | --- | --- | --- |
| Forward validation period and results | `ValidationRun`, `Report`, `PromotionDecision`, confidence history | No required missing capability. | Represent forward validation as validation runs with `FORWARD_VALIDATED` gate evidence. |
| Forward validation as confidence update | Confidence history model | No required missing capability. | Record confidence update events linked to validation IDs and experiment IDs. |
| Forward validation that blocks promotion | `ResearchDebtItem`, `PromotionDecision`, `Failure` if rejected | No required missing capability. | Use research debt when more forward evidence is required; use failure if evidence rejects the hypothesis. |
| Live observation-like forward notes | `Report`, `LiveDeployment`, `REFERENCES` | Minor relationship gap: clearer observation relationship would help. | Current model is sufficient; optional future relationship can improve readability. |

- **Owning registries**: `validation_engine/run_manifests`, `reports`, `promotion_engine`, advanced confidence history, `research debt` records.
- **Promotion stage**: `FORWARD_VALIDATED`.
- **Traceability**: Preserved.

### 10. Live Shadow Concepts

| Existing Knowledge | Current ROS Representation | Missing Capability | Recommendation |
| --- | --- | --- | --- |
| Live shadow as a pre-production phase | Promotion Engine stage `LIVE_SHADOW`, `LiveDeployment`, `ProductionCandidate` | No required missing capability. | Migrate live-shadow evidence as deployment or observation records linked to promotion decisions. |
| Shadow signals compared with offline expectations | `LiveDeployment`, `Report`, confidence history, `ValidationRun` if formalized | Minor relationship gap: explicit `OBSERVED_IN` would be clearer. | Use `Report REFERENCES LiveDeployment` and confidence history now; add relationship later only if needed. |
| Live shadow failure or mismatch | `Failure`, `ResearchDebtItem`, `PromotionDecision` rejection | No required missing capability. | Preserve mismatches as failures or blockers. |

- **Owning registries**: `promotion_engine`, `reports`, advanced graph `LiveDeployment`, `failure_registry`, research debt records.
- **Promotion stage**: `LIVE_SHADOW`.
- **Traceability**: Preserved, with minor improvement possible for observation relationships.

### 11. Failure Reports

| Existing Knowledge | Current ROS Representation | Missing Capability | Recommendation |
| --- | --- | --- | --- |
| Rejected hypotheses | `Failure` in `failure_registry/rejected/` | No required missing capability. | Migrate all serious failures with evidence, failed stage, population, metrics, and do-not-repeat notes. |
| Inconclusive results | `Failure` or `failure_registry/inconclusive/`, hypothesis status `INCONCLUSIVE` | No required missing capability. | Use inconclusive status when evidence is insufficient but not rejecting. |
| Repeated failure reasons | `Meta-research`, failure registry, knowledge graph traversal | No required missing capability. | Preserve standardized failure reasons to support meta-research. |
| Revisit conditions | Failure registry contract | No required missing capability. | Migrate as `revisit_conditions`. |

- **Owning registries**: `failure_registry`, `reports`, advanced graph `Failure`, meta-research layer.
- **Promotion stage**: Usually failed at `HYPOTHESIS_DEFINED`, `FEATURES_AVAILABLE`, `POPULATION_VALIDATED`, `EXPECTANCY_VALIDATED`, `STABILITY_VALIDATED`, `FORWARD_VALIDATED`, or `LIVE_SHADOW`.
- **Traceability**: Preserved.

### 12. Promotion Decisions

| Existing Knowledge | Current ROS Representation | Missing Capability | Recommendation |
| --- | --- | --- | --- |
| Gate movement from idea to production | Promotion Engine stages and `PromotionDecision` graph node | No required missing capability. | Migrate decisions as gate records with evidence links. |
| Approval or rejection rationale | `PromotionDecision`, `Report`, confidence history | No required missing capability. | Attach reports, validation IDs, and confidence history events. |
| Promotion blockers | `ResearchDebtItem BLOCKS PromotionDecision` | No required missing capability. | Use research debt for unresolved proof requirements. |
| Historical changes to promotion status | Versioning, `SUPERSEDES`, confidence history | No required missing capability. | Preserve status changes as versioned decisions or decision events. |

- **Owning registries**: `promotion_engine/gates`, `promotion_engine/decisions`, `promotion_engine/rollback_points`, advanced graph `PromotionDecision`.
- **Promotion stage**: Any stage from `IDEA` through `PRODUCTION`.
- **Traceability**: Preserved.

### 13. Research Debt

| Existing Knowledge | Current ROS Representation | Missing Capability | Recommendation |
| --- | --- | --- | --- |
| Known validation gaps such as walk-forward, larger population, cross-exchange validation, new datasets, new features, or better execution model | `ResearchDebtItem` | No required missing capability. | Migrate each unresolved limitation as a debt item linked to the affected hypothesis, edge, validation, or promotion decision. |
| Debt priority and blocking level | Research debt schema | No required missing capability. | Use `priority`, `estimated_cost`, `blocking_level`, dependencies, owner, and status. |
| Debt that prevents live promotion | `ResearchDebtItem BLOCKS PromotionDecision` | No required missing capability. | Explicitly link blockers to gate decisions. |

- **Owning registries**: Advanced graph `ResearchDebtItem`; future debt records can live under promotion or registry manifests until implementation chooses a path.
- **Promotion stage**: Any stage, especially `STABILITY_VALIDATED`, `FORWARD_VALIDATED`, `LIVE_SHADOW`, and `MICRO_LIVE`.
- **Traceability**: Preserved.

### 14. Rollback Points

| Existing Knowledge | Current ROS Representation | Missing Capability | Recommendation |
| --- | --- | --- | --- |
| Known stable rollback baseline | `promotion_engine/rollback_points/`, `PromotionDecision`, `Edge`, governance | Minor gap: no first-class advanced graph node type named `RollbackPoint`. | Keep as promotion metadata for migration; add `RollbackPoint` node only if migration exposes multiple rollback records that need independent traversal. |
| Rollback conditions | `PromotionDecision`, `ProductionCandidate`, `LiveDeployment`, governance | No required missing capability. | Store deactivation and rollback instructions in promotion or deployment records. |
| Rollback evidence | `ValidationRun`, `Report`, `Edge`, `PromotionDecision` | No required missing capability. | Link rollback point to source validation, scorecard, and report. |

- **Owning registries**: `promotion_engine/rollback_points`, `promotion_engine/decisions`, `edge_registry/promoted`, `reports`.
- **Promotion stage**: `PRODUCTION` or stable promoted baseline.
- **Traceability**: Mostly preserved; first-class rollback nodes would improve graph traversal but are not required to begin migration.

## Cross-Cutting Missing Capabilities

| Gap | Why It Matters | Severity | Recommendation |
| --- | --- | --- | --- |
| No explicit `ExecutionWorkflow` or `Runbook` object | MEXC live execution workflow is operational knowledge, not purely an edge, validation, report, or deployment. | Minor | Add runbook metadata under `LiveDeployment` or introduce a small runbook record when live migration begins. |
| No first-class `RollbackPoint` node in the advanced graph | Rollback points are present in Blueprint structure and governance, but less explicit in graph traversal. | Minor | Keep as `PromotionDecision` metadata now; add a node only if multiple rollback points need independent lifecycle tracking. |
| No explicit forward/live observation relationship | Forward validation and live shadow can be represented, but `REFERENCES` is generic. | Minor | Optional future relationship such as `OBSERVED_IN` or `FORWARD_OBSERVED` could improve readability. |
| Execution assumption metadata is not fully enumerated | Next Open Entry and MEXC workflow knowledge may depend on order timing, latency, fill assumptions, and slippage behavior. | Minor | Add execution-assumption fields to validation and live deployment manifests during implementation. |

No major structural gap was found.

## ROS Completeness Score

| Dimension | Qualitative Rating | Rationale |
| --- | --- | --- |
| Existing knowledge preservation | Strong | Pole, SL-C, Next Open Entry, Harmonic, ABCD, validation, failures, promotions, debt, and baseline knowledge can be represented. |
| Future detector scalability | Strong | Hypotheses, features, edge registry, shared validation, graph relationships, and experiment IDs support new detectors without ad-hoc structures. |
| Traceability | Strong with minor gaps | Experiment IDs, graph relationships, manifests, confidence history, and reports preserve traceability; live execution workflow and rollback nodes need minor strengthening. |
| Reproducibility | Strong | Dataset manifests, feature versions, validation run manifests, policy versions, and artifact manifests are sufficient for reproducible migration. |
| Decision support | Strong | Promotion gates, confidence evolution, research debt, edge composition, and meta-research support structured decisions. |
| Failure preservation | Very strong | Failure registry, failure graph node, revisit conditions, do-not-repeat notes, and meta-research directly preserve negative results. |
| Research reuse | Strong | Feature store contracts, dependency graph, universal experiment IDs, edge registry, and meta-research make historical work reusable. |

## Final Conclusion

**READY WITH MINOR GAPS**

The current ROS can represent the major historical knowledge families: Pole research, SL-C motif research, Next Open Entry findings, Harmonic research, ABCD research, the existing profitable Pole baseline, validation history, forward validation, live shadow concepts, failure reports, promotion decisions, research debt, and rollback points.

The evidence for readiness is that each historical category maps to existing ROS objects such as `Hypothesis`, `Feature`, `Experiment`, `ValidationRun`, `Edge`, `Failure`, `Report`, `PromotionDecision`, `ResearchDebtItem`, `ProductionCandidate`, and `LiveDeployment`. The registries and graph relationships already cover dependencies, validation, rejection, promotion, reporting, versioning, confidence evolution, and reproducibility.

The minor gaps are concentrated around operational live execution knowledge rather than research knowledge: MEXC workflow runbooks, explicit rollback graph nodes, and clearer forward/live observation relationships. These gaps do not require structural redesign before migration. They can be handled as small metadata additions or first-class records during implementation if migration proves they are needed.
