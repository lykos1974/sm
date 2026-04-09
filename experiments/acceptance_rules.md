# Experiment Acceptance Rules

## Baseline-First Policy
The current stable baseline is the default production research reference and rollback point. New experiments are evaluated against this baseline first, not in isolation.

## Required Metrics (Scorecard)
Every serious experiment must report:
- `candidate_rows_registered`
- `resolved_rows`
- `win_rate_non_ambiguous`
- `avg_realized_r_multiple`
- `total_realized_r_multiple`
- `TP1 -> TP2 conversion`

## Decision Definitions

### PROMOTE
Use **PROMOTE** only when results are clearly superior to baseline in overall quality and consistency, with no unacceptable regressions in key behavior.

### KEEP AS RESEARCH
Use **KEEP AS RESEARCH** when results are mixed, inconclusive, regime-dependent, or promising but not yet robust enough to replace baseline.

### DISCARD
Use **DISCARD** when performance degrades, risk/reward quality worsens materially, or behavior becomes less reliable than baseline.

## Evaluation Notes
- Win rate alone is insufficient for acceptance decisions.
- `TP1 -> TP2 conversion` matters substantially in this project and should carry significant weight.
- Shorts have not yet demonstrated stable edge and remain research-only until explicitly promoted.
