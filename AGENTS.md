# Repository Governance: PainaNdFear

## Mission Scope
This repository is a **trading research system**, not a generic coding sandbox. All work should preserve architectural intent, reproducibility, and rollback safety.

## Core Research Discipline
- One experiment = one idea = one isolated code change set.
- Preserve the stable baseline unless and until an experiment is explicitly promoted.
- Short-side logic is experimental unless explicitly promoted.
- Prefer minimal, reversible changes.
- If a change degrades results, mark it **DISCARD** and preserve baseline behavior.

## Stability and Interface Protection
- Do not silently rename public files or public functions.
- Do not change database schema unless explicitly required.
- Do not change exported metric names unless explicitly required.

Protected interfaces (must remain stable unless explicitly approved):
- `evaluate_pullback_retest_long(...)`
- `evaluate_pullback_retest_short(...)`
- `StrategyValidationStore.register_setup(...)`
- `StrategyValidationStore.update_pending_with_candle(...)`

## Baseline Rollback Policy
The current best-known profitable baseline is the rollback point and must be treated as stable until clearly outperformed and explicitly promoted.

Current stable rollback profile:
- Direction: LONG only
- `breakout_context`: `POST_BREAKOUT_PULLBACK`
- `pullback_quality`: `HEALTHY`
- `active_leg_boxes`: 2
- Non-extended only
- All symbols remain in scope unless explicitly changed
- No early breakeven logic in baseline

Reference baseline metrics:
- `candidate_rows_registered`: 98
- `resolved_rows`: 89
- `win_rate_non_ambiguous`: 0.3146
- `avg_realized_r_multiple`: 0.1919
- `total_realized_r_multiple`: 17.0763
- `TP1 -> TP2 conversion`: 0.9286

## Required Experiment Scorecard
Every serious experiment must report the following:
- `candidate_rows_registered`
- `resolved_rows`
- `win_rate_non_ambiguous`
- `avg_realized_r_multiple`
- `total_realized_r_multiple`
- `TP1 -> TP2 conversion`
