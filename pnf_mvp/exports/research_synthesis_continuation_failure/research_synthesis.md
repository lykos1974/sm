# 1) Executive Summary

- **What failed:** continuation-persistence and toxic-geometry filter hypotheses were not stable across holdouts; train improvements repeatedly degraded OOS, with frequent TP2 conversion damage.
- **What survived:** structural reversal propagation remained directionally consistent at the state-transition level (failure -> opposite WATCH emergence), with stronger evidence on failed LONG -> SHORT pathways.
- **Strongest surviving structural behavior:** failed LONG events, especially with DEEP pullback + LATE_EXTENSION context, showed elevated probability of subsequent SHORT WATCH promotion and faster structural progression.
- **Biggest caveats:** effects are not expectancy-validated, are vulnerable to overlap/selection artifacts, and show concentration risk (notably ETH-heavy slices), so this is not execution-ready.

# 2) Failed / Unstable Hypotheses

- **Continuation persistence instability:** persistence-style continuation conditions that looked strong in-sample did not retain strength under strict temporal separation.
- **Toxic geometry filtering instability:** geometry-based exclusion filters produced unstable lift and frequent sign-flips between train and OOS.
- **Train-only effects:** multiple uplift signals were likely train-specific and did not survive survival-separation style checks.
- **OOS collapse cases:** several conditioned cohorts collapsed in OOS despite apparent train robustness.
- **TP2 destruction issue:** aggressive filtering frequently reduced TP2 continuation depth even when headline hit rates improved.
- **ETH concentration risk:** strongest apparent improvements were often ETH-concentrated, reducing confidence in cross-symbol generality.

Explicit risk flags:
- **Geometry stability collapse** observed in holdout validation.
- **Failure filter instability** across temporal splits.
- **Optimizer illusion risk** from overlap-heavy recurring rows and narrow symbolic concentration.

# 3) Surviving Structural Behaviors

Only behaviors with survival evidence across OOS/asymmetry/propagation checks:

- **FAILED LONG -> SHORT asymmetry:** opposite-side emergence after failed LONG was stronger than the mirror failed SHORT -> LONG pathway.
- **WATCH propagation survives better than TP2 propagation:** opposite WATCH/CANDIDATE state emergence was more persistent than full TP2 completion.
- **SHORT WATCH promotion after failed LONG:** conditioned SHORT WATCH promotion improved versus unconditional SHORT WATCH baseline in the validated branch analyses.
- **DEEP pullback behavior:** deeper failure context tended to align with stronger opposite-side structural continuation than shallow contexts.
- **LATE_EXTENSION behavior:** late-extension failure context repeatedly appeared in the surviving reversal-propagation cluster.
- **ETH/SOL structural behavior:** ETH and SOL showed clearer/faster propagation signatures than BTC in the surviving slices.
- **BTC slower propagation:** BTC showed slower and weaker structural propagation, with larger structural-distance requirements before opposite progression completed.

# 4) PnF-Native Interpretation

- The signal is **structural-state dependent**, not candle-time dependent: outcomes are conditioned on PnF state transitions (FAILED continuation, opposite WATCH/CANDIDATE/TP2 emergence) and structural-distance progression.
- **Structural distance matters** because propagation appears path-dependent: closer opposite-side emergence in box-distance terms is associated with stronger continuation of the reversal process.
- A failed continuation likely updates the local structural prior: the failure event appears to shift subsequent state-transition probabilities toward opposite-side WATCH emergence before any execution layer is considered.

# 5) Most Important Metrics

Use these as the governing metrics from the branch analyses (not all are expectancy metrics):

- **Baseline SHORT WATCH -> TP2:** reference conversion for unconditional SHORT WATCH population.
- **Conditioned SHORT WATCH -> TP2 (post failed LONG):** key comparison metric; generally higher in surviving structural slices.
- **OOS persistence:** whether conditioned lift remained positive in OOS splits.
- **Median structural distance:** median distance from failure seed to opposite WATCH/CANDIDATE/TP2 events.
- **Symbol breakdowns:** ETH/SOL vs BTC contribution and concentration diagnostics.
- **Asymmetry ratios:** failed LONG -> SHORT propagation ratio relative to failed SHORT -> LONG.

Interpretation rule: prioritize metrics that survive OOS and asymmetry checks over train-only headline uplift.

# 6) Current Best Candidate Hypothesis

**Research-stage candidate (not production claim):**

FAILED LONG
+ DEEP pullback
+ LATE_EXTENSION
-> elevated SHORT WATCH -> TP2 probability
-> compressed structural distance to opposite-side progression

Constraints:
- This is **research-stage only**.
- **Not execution-ready**.
- **Not expectancy-validated** under realistic fills/costs.
- **Not production-ready** without broader robustness confirmation.

# 7) What Is Still Unknown

- Realistic execution expectancy after slippage, spread, and fees.
- Residual overlap effects and row-dependence leakage.
- Regime robustness (trend/volatility segmentation).
- Cost sensitivity under different liquidity regimes.
- Generalization beyond current symbol concentration.
- Long-horizon OOS stability under stricter holdouts.

# 8) Recommended Next Steps

Keep scope narrow and high-value:

1. Re-run top conditioned reversal cohorts with stricter, longer holdout windows.
2. Expand symbol set with explicit concentration caps and report per-symbol deltas.
3. Add minimal regime segmentation (trend/vol buckets) to test stability of asymmetry.
4. Perform execution-realism validation (slippage/cost/fill assumptions) before any expectancy interpretation.

