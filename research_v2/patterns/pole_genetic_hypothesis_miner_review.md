# Pole Genetic Hypothesis Miner Design Review

Date: 2026-06-04
Status: research-only review
Reviewed document: `research_v2/patterns/pole_genetic_hypothesis_miner_design.md`

## 1. Review Scope

This review audits the current Phase 0 design for the causal pole genetic hypothesis miner before any implementation work.
It does not implement the miner, modify detectors, modify live trading, modify production strategy, or promote any candidate.

Validated causal P+2 universe status used for this review:

| Metric | Value |
|---|---:|
| Observations | 4,042 |
| Trades | 4,023 |
| Wins | 1,143 |
| Losses | 2,606 |
| Break-even exits | 274 |
| Expectancy | +0.062516R/trade |
| Total R | +251.5R |
| Universe status | `UNIVERSE_MATCH` confirmed |

Prior research context:

- Historical non-causal Pole Motif rejected.
- P+4 true-birth causal motif rejected.
- Generic P+2 motif weak but positive.
- Segmentation suggests edge concentration in subsets.
- Intersection audit identified stronger sub-populations.

## 2. Summary Findings

The current design already contains strong causal guardrails, minimum trade thresholds, chronological validation, walk-forward validation, complexity penalties, symbol/time concentration penalties, and verdict categories.
However, it should be strengthened before implementation in four areas:

1. Add explicit elite stability scoring so top candidates are ranked by a frozen final score that includes validation, untouched test, walk-forward, and stability components.
2. Add explicit novelty and duplicate-candidate control so the population and final report do not fill with near-identical variants of the same rule family.
3. Add explicit bull/bear/sideways regime validation rather than relying only on trend/range genes, yearly buckets, and quarterly buckets.
4. Add a Monte Carlo robustness stage with bootstrap confidence intervals and reshuffled trade-path drawdown bands.

The safest implementation order remains Phase 1 deterministic exhaustive/random sampler first, then Phase 2 genetic miner.
Directly starting with a GA would increase overfit and debugging risk because the search space is enormous relative to the 4,023 executable trades.

## 3. PRESENT / PARTIAL / MISSING Table

| # | Protection | Status | Why |
|---:|---|---|---|
| 1 | Elite Stability Fitness | PRESENT | The design uses validation expectancy as search fitness, reserves test for final evaluation, requires walk-forward validation for robust candidates, and includes stability penalties. It is not train-expectancy driven. Add an explicit final elite score for clarity. |
| 2 | Novelty / Duplicate Candidate Control | PARTIAL | The design mentions diversity constraints and rule-family de-duplication, but it does not define rule similarity, clustering, novelty score, or population/report caps per cluster. |
| 3 | Survivorship Thresholds | PRESENT | The design defines hard minimum trades for full candidate, train, validation, test, and walk-forward folds, and rejects tiny segments. |
| 4 | Regime Stability | PARTIAL | The design allows pre-entry trend/range context and reports yearly/quarterly stability, but it does not explicitly require survival across bull, bear, and sideways regimes or penalize regime concentration by regime bucket. |
| 5 | Monte Carlo Robustness | PARTIAL | The design includes repeated grouped random splits and p10/p90 expectancy stress checks, but does not define bootstrap resampling, Monte Carlo trade reshuffling, confidence intervals, or percentile drawdown bands. |
| 6 | Candidate Promotion Gates | PARTIAL | The design defines allowed verdicts and robust-candidate conditions, but it does not provide a full quantitative gate table for `REJECT`/`WATCH`/`PROMISING`-style review stages or minimum improvement thresholds over baseline. |
| 7 | Search-Space Explosion Risk | PARTIAL | The design limits complexity and starts with Phase 1 sampler, but it does not estimate chromosome search-space size or explicitly state that full exhaustive enumeration is infeasible beyond low-order rules. |
| 8 | Time-Saving Review / Phase Order | PRESENT | The current plan starts with deterministic exhaustive/random sampler before the GA, which is safer and more auditable for the 4,023-trade universe. Strengthen by making Phase 1 exhaustive only for low-order rules. |

## 4. Item-by-Item Audit

### 4.1 Elite Stability Fitness

Status: PRESENT.

The current design is not driven by train expectancy alone.
It explicitly says the miner optimizes out-of-sample expectancy first and total R second, with penalties for complexity and fragility.
It also states that fitness should be computed on validation data during search and that test data should be used only once for final candidates.
The final fitness formula is based on `validation_expectancy_r`, normalized validation total R, complexity penalty, symbol penalty, time penalty, and stability penalty.
Robust verdict rules additionally require positive test expectancy, positive aggregate walk-forward expectancy, passing symbol/time concentration thresholds, and repeated split support.

This is directionally correct because train expectancy should be used only to fit feature bins, initialize or screen candidates, and estimate rough viability.
It should not rank final candidates.

Recommended design-only addition:

Add a distinct `elite_stability_score` used only after validation finalists are frozen and evaluated on test plus walk-forward.
This avoids ambiguity between search fitness and final elite ranking.

Suggested final elite score:

```text
elite_stability_score =
  0.35 * validation_expectancy_r
  + 0.30 * test_expectancy_r
  + 0.25 * walk_forward_expectancy_r
  + 0.10 * stability_adjusted_total_R_score
  - complexity_penalty
  - symbol_concentration_penalty
  - time_concentration_penalty
  - regime_concentration_penalty
  - monte_carlo_uncertainty_penalty
```

Rules:

- Train expectancy must not be an elite score component.
- Test may be used only after candidate selection is frozen.
- Walk-forward folds must be frozen before forward evaluation.
- A candidate cannot receive `ROBUST_CAUSAL_CANDIDATE` unless validation, test, and walk-forward expectancy are all positive.
- If validation is strong but test or walk-forward is weak, cap verdict at `PROMISING_BUT_UNSTABLE`.

### 4.2 Novelty / Duplicate Candidate Control

Status: PARTIAL.

The current design includes useful but incomplete language:

- Phase 2 includes elitism with diversity constraints.
- Anti-overfit protections include rule-family de-duplication.

That is not enough to prevent a GA population or final report from filling with minor variants such as:

- `ENA + HYPE + TAO`.
- `ENA + HYPE`.
- `ENA + HYPE + TAO + SHORT`.
- `ENA + HYPE + TAO + SHORT + retrace > 0.618`.

These variants may all describe the same concentrated sub-population.
Without explicit duplicate control, the miner can overstate evidence by reporting many correlated candidates as if they were independent discoveries.

Recommended design-only additions:

1. Add rule fingerprinting:
   - Canonicalize symbol sets, enum sets, interval bins, context bins, and execution settings.
   - Serialize the canonical predicate tree.
   - Hash the canonical representation as `rule_fingerprint`.

2. Add trade-set similarity:
   - For each candidate pair, compute Jaccard similarity over selected trade IDs.
   - Treat candidates with Jaccard similarity `>= 0.80` as near-duplicates.
   - Treat candidates with Jaccard similarity `>= 0.90` as duplicate family members unless execution parameters materially differ.

3. Add predicate similarity:
   - Compare symbol overlap, direction overlap, interval overlap, enum overlap, and execution-parameter distance.
   - Use predicate similarity when two rules select different trades only because of small threshold jitter.

4. Add cluster-level reporting:
   - Cluster finalists by trade-set similarity first, then predicate similarity.
   - Report one primary representative per cluster.
   - Include up to two alternates per cluster only if they materially improve out-of-sample stability or simplify the rule.

5. Add novelty pressure during GA:

```text
novelty_score = median_jaccard_distance_to_k_nearest_candidates
adjusted_generation_score = validation_fitness + novelty_lambda * novelty_score
```

Suggested defaults:

- `k = 10` nearest candidates.
- `novelty_lambda = 0.02R` to `0.05R`.
- Keep at least `30%` of each generation from distinct rule clusters.
- No more than `10%` of population may belong to one cluster.

6. Add final report controls:
   - `max_candidates_per_cluster = 1` for the headline table.
   - `max_alternates_per_cluster = 2` in appendix artifacts.
   - Cluster-level verdict cannot exceed the verdict of the representative candidate.

### 4.3 Survivorship Thresholds

Status: PRESENT.

The current design defines hard sample-size gates:

| Scope | Current design threshold |
|---|---:|
| Full candidate all-sample selected trades | 200 |
| Train selected trades | 100 |
| Validation selected trades | 50 |
| Test selected trades | 50 |
| Per walk-forward fold | 30 |

It also states that tiny candidates should be rejected or downgraded when full selected trades are below 200, validation trades below 50, test trades below 50, or bucket-level stability statistics are too sparse.
This is sufficient as a first design pass.

Recommended strengthening:

- Keep the current thresholds as hard defaults.
- Raise thresholds dynamically for complex candidates:

```text
required_full_trades = 200 + 25 * max(0, active_filter_count - 4)
required_validation_trades = 50 + 10 * max(0, active_filter_count - 4)
required_test_trades = 50 + 10 * max(0, active_filter_count - 4)
```

- Require at least 3 symbols for `ROBUST_CAUSAL_CANDIDATE`, unless the candidate is explicitly scoped as a single-symbol research artifact.
- Require at least 4 independent time buckets for `ROBUST_CAUSAL_CANDIDATE`.
- Treat single-symbol or two-quarter rules as exploratory even when expectancy is high.

### 4.4 Regime Stability

Status: PARTIAL.

The current design has regime-related components but not a complete regime-stability gate.
It allows `trend_range_context` as a causal context gene and includes yearly and quarterly stability metrics.
It also penalizes time concentration.
However, yearly and quarterly buckets are not equivalent to explicit bull/bear/sideways regime survival.
A rule could perform in one strong bull segment and one strong bear segment by calendar coincidence, or fail all sideways regimes while passing aggregate quarterly metrics.

Recommended design-only additions:

1. Define causal regime labels before mining:
   - `BULL`.
   - `BEAR`.
   - `SIDEWAYS`.
   - `TRANSITION`.
   - `UNKNOWN`.

2. Require all regime labels to be known at or before entry:
   - Regime labels must be computed from trailing market data only.
   - No future peak/trough labeling.
   - No full-sample hindsight regime segmentation.

3. Use two regime definitions:
   - Market-regime label from trailing benchmark or symbol trend.
   - Local P&F regime label from pre-entry trend/range/choppiness.

4. Add regime concentration metrics:

```text
regime_hhi = sum(regime_trade_share^2)
regime_total_R_hhi = sum(regime_total_R_share^2)
```

Suggested thresholds:

- `regime_hhi_limit = 0.55`.
- `top_regime_trade_share_reject = 0.70`.
- `top_regime_total_R_share_reject = 0.80`.

5. Add robustness gates:
   - Candidate must have non-negative expectancy in at least two of `BULL`, `BEAR`, and `SIDEWAYS`, if each has enough trades.
   - Candidate cannot receive `ROBUST_CAUSAL_CANDIDATE` if it is negative in all non-dominant regimes.
   - If a rule is intentionally regime-specific, label it as such and cap verdict at `PROMISING_BUT_UNSTABLE` until shadow validation confirms it.

### 4.5 Monte Carlo Robustness

Status: PARTIAL.

The current design includes repeated grouped random splits:

- 100 random seeds.
- Group by month or quarter.
- 70/30 train/evaluation split over time buckets.
- Report median, p10, and p90 evaluation expectancy.

This is helpful, but it is not a full Monte Carlo robustness stage.
It does not explicitly include bootstrap resampling, confidence intervals, reshuffled trade-path drawdowns, or percentile expectancy bands for final candidates.

Recommended research-only robustness stage:

Add a post-finalist `Monte Carlo Robustness` stage after chronological test and before final verdict assignment.

Required calculations:

1. Bootstrap expectancy confidence interval:
   - Resample selected trades with replacement.
   - Use at least 5,000 bootstrap samples for finalists.
   - Report mean, median, p05, p10, p90, p95 expectancy.
   - Report 90% and 95% confidence intervals for expectancy and total R.

2. Block bootstrap by time:
   - Resample monthly or quarterly blocks with replacement.
   - Preserve clustered market conditions better than iid trade resampling.
   - Report block-bootstrap p10 expectancy.

3. Monte Carlo trade-path reshuffling:
   - Shuffle realized R order across selected trades.
   - Preserve trade outcomes but vary path dependency.
   - Report p50/p90/p95 max drawdown R.
   - Report probability of exceeding historical max drawdown by 25% and 50%.

4. Leave-one-group-out stress tests:
   - Remove best symbol.
   - Remove best quarter.
   - Remove best regime.
   - Remove best month if monthly sample is adequate.
   - Recompute expectancy and total R.

Suggested robustness gates:

- `bootstrap_p10_expectancy_r >= 0.00R` for robust candidates.
- `block_bootstrap_p10_expectancy_r >= -0.02R` at minimum.
- `leave_best_symbol_out_expectancy_r >= 0.00R`.
- `leave_best_quarter_out_expectancy_r >= 0.00R`.
- `mc_p95_max_drawdown_R` must be compatible with intended shadow-validation risk limits.

If these fail, cap verdict at `PROMISING_BUT_UNSTABLE` or `OVERFIT_LIKELY` depending on severity.

### 4.6 Candidate Promotion Gates

Status: PARTIAL.

The current design defines allowed verdicts:

- `ROBUST_CAUSAL_CANDIDATE`.
- `PROMISING_BUT_UNSTABLE`.
- `OVERFIT_LIKELY`.
- `NO_EDGE_FOUND`.
- `INSUFFICIENT_DATA`.

It also describes robust-candidate requirements, including causal validation, minimum trades, positive validation/test/walk-forward expectancy, concentration thresholds, best-symbol and best-quarter removal tests, and repeated random split support.
This is good, but the criteria are not yet organized as an explicit gate table with quantitative promotion thresholds.
Also, the user's example categories include `REJECT`, `WATCH`, `PROMISING`, and `ROBUST_CAUSAL_CANDIDATE`; the current design has no explicit `WATCH` stage.

Recommended design-only gate table:

| Review stage | Internal verdict mapping | Required criteria |
|---|---|---|
| `REJECT` | `OVERFIT_LIKELY`, `NO_EDGE_FOUND`, or `INSUFFICIENT_DATA` | Causality fails, sample thresholds fail, validation expectancy <= baseline noise, test expectancy negative, or severe concentration. |
| `WATCH` | `PROMISING_BUT_UNSTABLE` | Causality passes, validation expectancy positive, but test/walk-forward/Monte Carlo/regime stability is mixed. Shadow-only review may be justified. |
| `PROMISING` | `PROMISING_BUT_UNSTABLE` | Validation and test expectancy positive, walk-forward non-negative, no severe concentration, but one robustness gate remains weak. |
| `ROBUST_CAUSAL_CANDIDATE` | `ROBUST_CAUSAL_CANDIDATE` | All hard gates pass, and out-of-sample expectancy is materially better than the generic P+2 baseline. |

Suggested quantitative hard gates for `ROBUST_CAUSAL_CANDIDATE`:

| Gate | Threshold |
|---|---:|
| Causal validator | Pass |
| Full selected trades | `>= 200`, dynamically higher for complex rules |
| Train selected trades | `>= 100` |
| Validation selected trades | `>= 50` |
| Test selected trades | `>= 50` |
| Walk-forward valid folds | `>= 5` if data permits |
| Validation expectancy | `> +0.062516R` generic P+2 baseline, preferably by `>= +0.05R` absolute |
| Test expectancy | `> 0R` and not below validation by more than `0.10R` |
| Walk-forward expectancy | `> 0R` |
| Bootstrap p10 expectancy | `>= 0R` |
| Top symbol trade share | `<= 65%` |
| Top quarter trade share | `<= 50%` |
| Best-symbol removal expectancy | `>= 0R` |
| Best-quarter removal expectancy | `>= 0R` |
| Active filters | `<= 8`, with complexity penalty above simple budget |

Important: even `ROBUST_CAUSAL_CANDIDATE` remains a research verdict.
It is not a production promotion and should lead only to candidate export or shadow validation unless a separate approved implementation task exists.

### 4.7 Search-Space Explosion Risk

Status: PARTIAL.

The current design has a complexity penalty, hard active-filter cap, coarse bins, and a Phase 1 sampler before the GA.
However, it does not estimate search-space size.
The full chromosome search space is too large for exhaustive enumeration.

Approximate full-space estimate using the current suggested bins:

| Component | Rough choices |
|---|---:|
| Symbol subset over 7 symbols | 128 including `ALL`/empty-equivalent handling |
| Direction | 3 |
| Pole boxes interval/disabled | 11 |
| Pole duration interval/disabled | 11 |
| Pole velocity interval/disabled | 11 |
| Relative pole size enum-set/disabled | 16 |
| Reversal boxes interval/disabled | 11 |
| Retrace ratio interval/disabled | 11 |
| Retrace quality enum-set/disabled | 32 |
| Trend/range enum-set/disabled | 32 |
| Alternation interval/disabled | 11 |
| Choppiness enum-set/disabled | 16 |
| Lookback columns | 4 |
| Execution variants | 500 |

Approximate total:

```text
128 * 3 * 11^5 * 16 * 32 * 32 * 11 * 16 * 4 * 500
= approximately 3.6e17 candidate combinations
```

This excludes threshold jitter, alternative family definitions, future additional context genes, and chromosome-equivalent duplicates.
Full exhaustive search is therefore infeasible.

Phase 1 should remain, but its name should be interpreted precisely:

- Exhaustive only for zero-filter, one-filter, and selected two-filter coarse rules.
- Deterministic random sampler for higher-order rules.
- Include hand-seeded prior-hint candidates such as `SHORT + retrace > 0.618 + NEAR_RECENT_AVG`, but label them as priors.
- Use Phase 1 to validate data plumbing, causality, metrics, and obvious low-complexity segments.
- Do not attempt exhaustive enumeration of the full chromosome space.

Recommendation: keep Phase 1 first, then Phase 2 GA.
Do not move directly to GA.

### 4.8 Time-Saving Review / Implementation Order

Status: PRESENT.

Given the validated 4,023-trade universe, Phase 1 deterministic sampler first is safer and more likely to avoid overfitting than a direct GA.

Reasons:

1. Auditability:
   - A deterministic sampler makes it easier to verify `UNIVERSE_MATCH`, causal feature lineage, split logic, and metric calculations before stochastic search begins.

2. Baseline calibration:
   - The generic P+2 motif is weak but positive.
   - Low-order deterministic scans reveal whether the apparent edge concentration is broad or just a narrow interaction artifact.

3. Overfit control:
   - A GA can rapidly discover high-expectancy tiny segments unless the validator, gates, and duplicate controls are already proven.
   - Phase 1 exercises these gates in a transparent way.

4. Debugging speed:
   - If Phase 1 cannot reproduce expected metrics, a GA result is not trustworthy.
   - Deterministic outputs are easier to diff across code changes and seeds.

5. Search-space scale:
   - The full space is approximately `3.6e17` combinations using coarse assumptions.
   - Direct GA is unavoidable eventually for higher-order interactions, but it should not be the first implemented miner.

Recommended path:

1. Phase 1A: universe reproduction and causal feature registry.
2. Phase 1B: deterministic zero-/one-/two-filter scan.
3. Phase 1C: deterministic random multi-filter sampler with fixed seeds.
4. Phase 1D: duplicate clustering and Monte Carlo robustness for top sampler candidates.
5. Phase 2: GA only after Phase 1 artifacts and validation gates are stable.

## 5. Recommended Design Additions

The current design should be amended before implementation with the following research-only additions.

### 5.1 Add Elite Stability Score Section

Add a section after the current fitness function defining:

- `validation_search_fitness` for GA selection.
- `elite_stability_score` for frozen finalists.
- Explicit ban on train expectancy in final elite ranking.
- Explicit cap on verdict when test or walk-forward expectancy fails.

### 5.2 Add Novelty and Candidate Clustering Section

Add a section defining:

- Canonical `rule_fingerprint`.
- Jaccard trade-set similarity.
- Predicate similarity.
- Candidate clustering.
- Population diversity quotas.
- Final report caps per cluster.
- Novelty score and novelty-adjusted generation score.

### 5.3 Add Regime Validation Section

Add a section defining:

- Causal bull/bear/sideways/transition/unknown labels.
- Regime known-at rules.
- Regime concentration metrics.
- Non-dominant regime survival checks.
- Caps for intentionally regime-specific candidates.

### 5.4 Add Monte Carlo Robustness Section

Add a stage after test/walk-forward finalist selection defining:

- Trade-level bootstrap.
- Block bootstrap by month/quarter.
- Monte Carlo trade-path reshuffling.
- Confidence intervals and percentile bands.
- Leave-best-symbol/quarter/regime/month stress tests.

### 5.5 Add Explicit Promotion Gate Table

Add a table mapping human review stages to allowed verdicts:

- `REJECT` -> `OVERFIT_LIKELY`, `NO_EDGE_FOUND`, or `INSUFFICIENT_DATA`.
- `WATCH` -> weak `PROMISING_BUT_UNSTABLE`.
- `PROMISING` -> strong `PROMISING_BUT_UNSTABLE`.
- `ROBUST_CAUSAL_CANDIDATE` -> all hard gates pass.

Keep the allowed machine verdicts unchanged unless the project explicitly chooses to add `WATCH` and `REJECT` as exported verdict names.

### 5.6 Clarify Phase 1 Exhaustive Scope

Update Phase 1 wording to say:

- Exhaustive enumeration is limited to zero-filter, one-filter, and selected two-filter coarse rules.
- Higher-order candidates use deterministic random sampling.
- Full chromosome exhaustive search is explicitly out of scope due to search-space explosion.

## 6. Final Recommendation

Keep the current Phase 1 deterministic exhaustive/random sampler first.
Do not move directly to GA.

The safer implementation sequence is:

1. Phase 1A: reproduce `UNIVERSE_MATCH` with observations/trades/wins/losses/BE/expectancy/total_R.
2. Phase 1B: build causal feature registry and denylist validator.
3. Phase 1C: run deterministic low-order scans.
4. Phase 1D: add candidate clustering and duplicate control.
5. Phase 1E: add Monte Carlo robustness for sampler finalists.
6. Phase 2: introduce GA with novelty pressure and cluster quotas.
7. Phase 3: walk-forward validation.
8. Phase 4: candidate export.
9. Phase 5: shadow validation only.

This order maximizes reproducibility, exposes overfit early, and preserves the repository's research discipline: one experiment, one idea, isolated change set, stable baseline untouched.
