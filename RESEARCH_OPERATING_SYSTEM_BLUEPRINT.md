# Research Operating System Blueprint

## 1. Purpose

The PnF project should introduce a reusable Research Operating System (ROS) before scaling into many new detectors or edge hypotheses. The purpose is to turn research from isolated one-off investigations into a repeatable, auditable decision pipeline.

The ROS exists to:

- Avoid repeated one-off research that cannot be fairly compared later.
- Avoid recomputation by reusing frozen datasets, shared features, cached labels, and prior validation outputs.
- Preserve discoveries as versioned edges with known scope, dependencies, strengths, and weaknesses.
- Preserve failures so rejected ideas do not get rediscovered and retested without new evidence.
- Compare hypotheses fairly using the same data, costs, ambiguity policy, splits, walk-forward logic, and metrics.
- Reduce time to validate or reject future edges by making inputs, dependencies, outputs, and promotion gates explicit.

This is an architecture and operating model only. It does not modify existing Pole, ABCD, Harmonic, or MEXC live trader behavior.

## 2. Proposed Folder Structure

Future namespace only; do not create folders until implementation is explicitly approved.

```text
research_v3/
  feature_store/
    manifests/
    generators/
    schemas/
    cache_index/
  hypothesis_registry/
    active/
    backlog/
    archived/
  edge_registry/
    validated/
    promoted/
    deprecated/
  failure_registry/
    rejected/
    inconclusive/
    do_not_repeat/
  validation_engine/
    configs/
    policies/
    run_manifests/
    metric_specs/
  promotion_engine/
    gates/
    decisions/
    rollback_points/
  manifests/
    datasets/
    artifacts/
    runs/
  reports/
    comparisons/
    scorecards/
    postmortems/
  dashboards/
    static/
    interactive_specs/
  adapters/
    pole/
    abcd/
    harmonic/
    mexc_shadow/
```

## 3. Core Concepts

- **Feature**: A versioned, reusable derived attribute computed from raw or intermediate market data. A feature has explicit inputs, schema, dataset compatibility, null policy, and recomputation rules.
- **Hypothesis**: A testable research claim about a market mechanism that may produce positive expectancy under defined conditions.
- **Edge**: A hypothesis that has passed validation gates with documented expectancy, stability, robustness, population, and known weaknesses.
- **Failed idea**: A hypothesis or implementation path rejected by evidence, insufficient population, unstable performance, excessive cost, or invalid assumptions.
- **Dataset manifest**: A versioned description of a frozen dataset, including symbols, date ranges, source, cleaning rules, exclusions, checksums, and compatibility constraints.
- **Validation run**: A reproducible evaluation of one or more hypotheses against a specific frozen dataset, feature set, policy set, and metric definition.
- **Promotion gate**: A required decision checkpoint that moves an idea toward or away from production eligibility based on documented evidence.
- **Production candidate**: An edge that has passed offline validation, forward validation, and live-shadow requirements and is ready for micro-live evaluation with rollback controls.

## 4. Feature Store Contract

Every feature must have a manifest with the following metadata:

| Field | Requirement |
| --- | --- |
| `feature_id` | Stable unique identifier. |
| `version` | Semantic or date-based version; immutable once published. |
| `description` | What the feature measures and why it exists. |
| `input_dependencies` | Raw datasets, upstream features, labels, parameters, and policy versions. |
| `dataset_compatibility` | Dataset manifests and candle intervals the feature supports. |
| `generator_reference` | Code/module/notebook reference used to produce it once code exists. |
| `incremental_support` | Whether new candles can update the feature without full recomputation. |
| `output_schema` | Column names, types, units, allowed ranges, and index keys. |
| `null_handling` | When nulls are allowed, how they are encoded, and whether they invalidate rows. |
| `recomputation_policy` | Conditions requiring rebuild: upstream changes, bug fixes, dataset changes, or schema changes. |
| `validation_checks` | Determinism checks, row-count checks, null-rate thresholds, range checks, and sample audits. |

Feature manifests should distinguish stable features from experimental features. Stable features should not be recomputed unless their manifest policy requires it.

## 5. Hypothesis Registry Contract

Every hypothesis must be registered before serious validation.

| Field | Requirement |
| --- | --- |
| `hypothesis_id` | Stable unique identifier. |
| `name` | Short human-readable name. |
| `description` | Precise claim being tested. |
| `expected_mechanism` | Why the edge should exist in market structure or participant behavior. |
| `required_features` | Feature IDs and versions needed for evaluation. |
| `expected_population` | Expected symbols, regimes, setups, directions, and minimum sample size. |
| `validation_cost` | Estimated compute/runtime/manual review cost. |
| `expected_information_gain` | Expected reduction in uncertainty even if rejected. |
| `priority_score` | Ranking score from the research prioritization model. |
| `kill_criteria` | Conditions that stop work early. |
| `owner_status` | Owner, lifecycle status, and review state. |
| `next_action` | The smallest concrete next step. |

Recommended statuses: `IDEA`, `HYPOTHESIS_DEFINED`, `BLOCKED_INPUTS`, `READY_FOR_VALIDATION`, `VALIDATING`, `PASSED`, `FAILED`, `INCONCLUSIVE`, `ARCHIVED`.

## 6. Edge Registry Contract

Every validated edge must have an edge record.

| Field | Requirement |
| --- | --- |
| `edge_id` | Stable unique identifier. |
| `source_hypothesis` | Hypothesis ID and validation run IDs supporting the edge. |
| `status` | `VALIDATED`, `FORWARD_TESTING`, `LIVE_SHADOW`, `MICRO_LIVE`, `PRODUCTION`, `DEPRECATED`, or `DISCARD`. |
| `validated_symbols` | Symbols included and excluded. |
| `date_ranges` | In-sample, out-of-sample, walk-forward, and forward ranges. |
| `population` | Number of candidates, resolved outcomes, regimes, and setup filters. |
| `expectancy` | Mean/median realized R, total R, win rate, payoff profile, and confidence bands. |
| `drawdown` | Max drawdown, drawdown duration, tail loss, and consecutive loss behavior. |
| `stability` | Performance across years, symbols, regimes, and folds. |
| `robustness` | Sensitivity to costs, slippage, ambiguity policy, and parameter perturbations. |
| `dependencies` | Dataset, feature, policy, adapter, and execution dependencies. |
| `known_weaknesses` | Conditions where edge degrades or should be disabled. |
| `promotion_status` | Current gate, approver, evidence, and rollback point. |
| `live_eligibility` | Whether the edge may enter shadow, micro-live, or production. |

Any edge compared with the current Pole baseline must report the required experiment scorecard: `candidate_rows_registered`, `resolved_rows`, `win_rate_non_ambiguous`, `avg_realized_r_multiple`, `total_realized_r_multiple`, and `TP1 -> TP2 conversion`.

## 7. Failure Registry Contract

Every rejected or abandoned serious idea must leave a failure record.

| Field | Requirement |
| --- | --- |
| `failure_id` | Stable unique identifier. |
| `hypothesis_id` | Linked hypothesis. |
| `failure_reason` | Primary rejection reason. |
| `evidence` | Validation runs, reports, charts, or review notes supporting rejection. |
| `failed_stage` | Gate or lifecycle stage where the idea failed. |
| `population` | Candidate count, symbol coverage, date range, and filters. |
| `key_metrics` | Core metrics, including required scorecard when applicable. |
| `may_be_revisited` | Boolean or conditional state. |
| `revisit_conditions` | Specific new data, feature, regime, or mechanism required before retest. |
| `do_not_repeat_notes` | Warnings to prevent duplicate work. |

Failure records are research assets. They should never be deleted to make reports look cleaner.

## 8. Shared Validation Engine Design

The validation engine should evaluate many hypotheses through one common pipeline:

1. Load a frozen dataset by dataset manifest ID.
2. Resolve feature dependencies by feature ID and version.
3. Apply one cost/slippage model for all hypotheses in a comparison set.
4. Apply one ambiguity policy for all outcomes in a comparison set.
5. Apply identical symbol/year splits and walk-forward logic.
6. Replay candles once when possible, emitting shared events and candidate streams.
7. Run each hypothesis adapter against the shared event stream.
8. Resolve outcomes through the shared outcome engine.
9. Emit standardized metrics and artifacts.
10. Write validation run manifests for every output.

Core output semantics:

- **PASS**: Meets minimum population, expectancy, stability, robustness, and data-quality requirements.
- **FAIL**: Violates kill criteria, underperforms baseline, lacks sufficient population, or fails validation checks.
- **INCONCLUSIVE**: Evidence is insufficient, unstable, or blocked by data/features, but the mechanism is not disproven.

Comparison reports must show both absolute performance and delta versus the existing proven Pole baseline when relevant.

## 9. Parallel Hypothesis Evaluation

Multiple hypotheses can be evaluated together without duplicating work by using shared intermediate artifacts:

- **Shared base features**: Compute candles, volatility, trend, liquidity, session, range, and structure primitives once.
- **Shared structural labels**: Reuse swing points, breakouts, pullbacks, retests, leg counts, and regime tags across detectors.
- **Shared candle replay**: Stream candles once and allow hypothesis adapters to subscribe to common events.
- **Shared outcome engine**: Resolve entries, stops, targets, ambiguity, costs, and realized R using one implementation.
- **Incremental updates**: Append new periods to compatible artifacts rather than rebuilding all history.
- **Caching**: Cache deterministic feature outputs, label sets, candidate populations, and outcome tables by manifest hash.
- **Dependency graph**: Track which hypotheses depend on which features, datasets, policies, and adapters so only invalidated nodes rerun.

The default research path should be: update manifests, reuse stable artifacts, compute only missing dependencies, then run the smallest validation set that answers the question.

## 10. Promotion Engine

Promotion is a gated lifecycle, not an informal decision.

```text
IDEA
  ↓
HYPOTHESIS_DEFINED
  ↓
FEATURES_AVAILABLE
  ↓
POPULATION_VALIDATED
  ↓
EXPECTANCY_VALIDATED
  ↓
STABILITY_VALIDATED
  ↓
FORWARD_VALIDATED
  ↓
LIVE_SHADOW
  ↓
MICRO_LIVE
  ↓
PRODUCTION
```

Gate expectations:

- `IDEA`: Mechanism is plausible enough to register.
- `HYPOTHESIS_DEFINED`: Claim, population, features, kill criteria, and next action are explicit.
- `FEATURES_AVAILABLE`: Required feature manifests exist and pass checks.
- `POPULATION_VALIDATED`: Candidate count and coverage are sufficient.
- `EXPECTANCY_VALIDATED`: Net expectancy is positive after costs and ambiguity policy.
- `STABILITY_VALIDATED`: Performance is not concentrated in one symbol, year, or regime without explanation.
- `FORWARD_VALIDATED`: Behavior remains acceptable on unseen or newly appended data.
- `LIVE_SHADOW`: Signals run live without execution and match offline expectations.
- `MICRO_LIVE`: Small capital exposure validates operational assumptions.
- `PRODUCTION`: Edge has approval, rollback point, monitoring, and deactivation rules.

## 11. Priority Scoring

Research priority should be quantitative enough to prevent bias but simple enough to maintain.

Suggested score:

```text
priority_score =
  2.0 * expected_information_gain
+ 1.5 * probability_of_edge
+ 1.2 * dataset_readiness
+ 1.0 * overlap_with_existing_features
+ 1.0 * novelty
- 1.2 * implementation_cost
- 1.2 * validation_cost
- 1.5 * downside_risk
- 0.8 * time_to_answer
```

Scoring dimensions:

- **Expected information gain**: How much the result will clarify future research direction.
- **Implementation cost**: Work needed to express the hypothesis and required features.
- **Validation cost**: Compute, review, and reporting burden.
- **Dataset readiness**: Whether frozen datasets already support the test.
- **Overlap with existing features**: Higher score when stable features already cover most inputs.
- **Novelty**: Whether the idea explores a distinct mechanism rather than a small parameter variant.
- **Probability of edge**: Prior belief based on market structure, prior evidence, or related validated edges.
- **Downside risk**: Risk of contaminating baseline, encouraging overfit, or consuming major effort.
- **Time-to-answer**: Lower time-to-answer increases practical priority.

Priority scores choose research order only; they do not replace validation gates.

## 12. Time-Saving Rules

- Prefer incremental computation.
- No full reruns unless an invalidated dependency requires it.
- Reuse frozen datasets.
- Reuse existing artifacts.
- Do not recompute stable features when manifests and checksums still match.
- Write manifests for every output.
- Fail fast when inputs, features, manifests, policies, or schemas are missing.
- Preserve negative results in the failure registry.
- Compare serious candidates against the existing proven Pole baseline.
- Cache by manifest hash rather than by informal file names.
- Separate feature generation from hypothesis evaluation.
- Separate validation from promotion.

## 13. Governance

- No production promotion without documented validation.
- No strategy changes without a rollback point.
- No hidden parameter optimization.
- No untracked manual artifact.
- No deleting previous results.
- Every failed serious idea must leave a failure record.
- Every promising idea must have a promotion record.
- Every validation run must declare dataset, feature, policy, cost, slippage, ambiguity, and split versions.
- Every promoted candidate must include monitoring, disable criteria, and rollback instructions.
- Existing Pole, ABCD, Harmonic, and MEXC live trader behavior remains unchanged unless explicitly approved in a separate implementation change.

## 14. Minimal Implementation Roadmap

| Phase | Goal | Deliverables | No-code or code | Risks | Expected time savings |
| --- | --- | --- | --- | --- | --- |
| `ROS-0` | Inventory existing research artifacts. | Artifact index, known baselines, current datasets, reports, scripts, and gaps. | No-code. | Inventory may expose inconsistent historical outputs. | Prevents duplicate searches and identifies reusable assets. |
| `ROS-1` | Start registries as markdown/json only. | Hypothesis, edge, and failure templates; initial registry files. | Mostly no-code. | Process friction if templates are too heavy. | Captures decisions immediately without waiting for tooling. |
| `ROS-2` | Define feature manifest schema. | Feature manifest template, required metadata, checksum convention, compatibility rules. | Mostly no-code; optional schema validation later. | Over-modeling features before usage patterns are known. | Prevents recomputation and clarifies dependencies. |
| `ROS-3` | Define validation run manifest schema. | Run manifest template, metric contract, policy version fields, PASS/FAIL/INCONCLUSIVE convention. | Mostly no-code; optional schema validation later. | Legacy runs may not have enough metadata. | Makes comparisons reproducible and auditable. |
| `ROS-4` | Produce first multi-hypothesis comparison report. | One standardized report comparing at least two registered hypotheses or one hypothesis versus baseline. | Light code only if needed for reporting. | Premature automation may distract from contract quality. | Validates the operating model and reveals missing fields. |
| `ROS-5` | Add dashboard/reporting layer. | Static dashboard or report index summarizing hypotheses, edges, failures, and gate status. | Code. | Dashboard can become cosmetic if source manifests are weak. | Accelerates portfolio-level research decisions. |

## 15. Final Recommendation

Yes. The project should build the ROS foundation before starting many new detectors.

The recommended near-term path is `ROS-0` through `ROS-3` first because those phases are low-risk, mostly no-code, and create immediate leverage: inventory, registries, feature manifests, and validation run manifests. Only after those contracts exist should the project scale into parallel detector research. This preserves the current profitable Pole baseline, prevents hidden overfit, reduces recomputation, and creates a fair promotion path for future edges.
