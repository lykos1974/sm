# Ratio Predictive Research Report

## Scope
- Research only: no detector, no strategy, no expectancy, and no production changes.
- Input: `research_v2/patterns/harmonic_swing_threshold_local_v2/harmonic_reactions_by_threshold.csv`.
- Threshold: `SLOW` only.
- Buckets are lower-inclusive and upper-exclusive.
- Next confirmed swing is the first later same-symbol SLOW `CONFIRMING` reaction; rows without a later confirmed swing are counted as unresolved and excluded from average/median/frequency denominators.
- Continuation means the next confirmed swing returns to the current active-swing direction; reversal means it confirms in the reaction/candidate direction.

## Dataset Coverage
- SLOW reactions: 15
- SLOW confirming reactions: 7
- SLOW internal reactions: 8
- Reactions with a later confirmed swing: 8
- Reactions without a later confirmed swing: 7

## Bucket Results
| Bucket | Raw reactions | Measured count | Avg next swing | Median next swing | Continuation freq | Reversal freq | Unresolved |
|---|---|---|---|---|---|---|---|
| 0.20-0.30 | 3 | 1 | 7 | 7 | 0 | 1 | 2 |
| 0.30-0.40 | 3 | 1 | 11 | 11 | 1 | 0 | 2 |
| 0.40-0.50 | 0 | 0 |  |  |  |  | 0 |
| 0.50-0.60 | 0 | 0 |  |  |  |  | 0 |
| 0.60-0.70 | 2 | 2 | 10.5 | 10.5 | 0.5 | 0.5 | 0 |
| 0.70-0.80 | 0 | 0 |  |  |  |  | 0 |
| 0.90-1.10 | 2 | 2 | 15.5 | 15.5 | 0.5 | 0.5 | 0 |
| 1.20-1.35 | 1 | 0 |  |  |  |  | 1 |
| 1.55-1.70 | 1 | 0 |  |  |  |  | 1 |
| 2.00-2.10 | 1 | 1 | 5 | 5 | 0 | 1 | 0 |

## Material Behavior Assessment
- Measured observations are sparse for robust predictive claims; the largest requested bucket has 2 measured reactions (3 raw reactions).
- The largest observed average next swing is bucket 0.90-1.10 at 15.5 boxes, while the smallest is bucket 2.00-2.10 at 5 boxes.
- Some buckets appear one-sided, but only in cells below the measured-count robustness floor of 30.
- Research conclusion: treat the bucket table as descriptive evidence only; require symbol and year stability checks before using the ratio split for further research.

## Answer
See the material behavior assessment and stability report for whether observed bucket separation is supported by the measured-count, symbol, and year checks.
