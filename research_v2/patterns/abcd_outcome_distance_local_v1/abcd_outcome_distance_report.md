# AB=CD Phase 3 Outcome Distance Audit Report

## Scope
- Research-only audit of the distance from D to the currently used next confirmed pivot outcome.
- Source detail: geometry candidates `H:/pnf screener/sm_repo/research_v2/patterns/abcd_geometry_local_v1/abcd_geometry_candidates.csv` with next pivots from `H:/pnf screener/sm_repo/research_v2/patterns/VALIDATED_harmonic_swing_threshold_local_v3/harmonic_reactions_by_threshold.csv`.
- No outcome logic, geometry logic, continuation/reversal semantics, strategy, detector, scanner, entries, exits, stops, targets, expectancy, or PnL logic was changed or created.

## Required Answers
1. **Is next confirmed pivot usually the immediate next structural pivot?** Yes; pct_column_distance_1=0.5425831202 across 7820 measured rows.
2. **Is column_distance almost always 1?** No; median_column_distance=1, p90_column_distance=11.
3. **Is the 100% continuation result likely tautological?** Not proven solely by this audit; the current metric uses the first confirmed pivot after D, and this audit does not show an almost-always-one-column distance.
4. **Is current Phase 3 outcome metric meaningful as post-D behavior?** Partially; distance is not overwhelmingly immediate, but it still only measures the first confirmed pivot.
5. **Should future outcome research use a later horizon?** Yes. Later-horizon structural audits should consider next 3 confirmed pivots, next 5 confirmed pivots, max favorable/adverse boxes after D, and first break of B/C structural level.

## Root Cause Assessment
- Root cause not proven as fully tautological at the 95% one-column threshold, but the metric remains structurally constrained to the first confirmed pivot after D.

## All Rows Summary
| Cohort | Count | Median column distance | Average column distance | P25 | P75 | P90 | Pct distance = 1 | Pct distance <= 2 | Pct distance <= 3 | Median time ms | Average time ms | P90 time ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| ALL | 7820 | 1 | 6.7537084399 | 1 | 3 | 11 | 0.5425831202 | 0.5425831202 | 0.7514066496 | 5580000 | 25016416.8797953948 | 38820000 |
| SYM_0_90_1_10 | 854 | 1 | 4.5573770492 | 1 | 3 | 7 | 0.5327868852 | 0.5327868852 | 0.7763466042 | 5850000 | 16465573.7704918031 | 34764000.0000000298 |
| EXT_1_20_1_35 | 548 | 1 | 5.7664233577 | 1 | 3 | 9 | 0.5656934307 | 0.5656934307 | 0.7773722628 | 5250000 | 19561094.8905109502 | 38592000.0000000075 |
| EXT_1_55_1_70 | 328 | 1 | 8.8109756098 | 1 | 3 | 11 | 0.5426829268 | 0.5426829268 | 0.7591463415 | 4860000 | 39015000 | 40242000.0000000596 |
| OTHER | 6090 | 1 | 7.0397372742 | 1 | 5 | 11 | 0.5418719212 | 0.5418719212 | 0.7451559934 | 5640000 | 25952443.3497536927 | 38946000.0000000224 |

## Research Guardrail
Strictly structural meaning audit only; no trading or profitability conclusion is made.
