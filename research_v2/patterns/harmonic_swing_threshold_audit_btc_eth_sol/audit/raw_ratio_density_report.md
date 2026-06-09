# Raw Ratio Density Report

Research-only audit. This report uses raw `reaction_ratio` values only; it does not use nearest harmonic assignment, harmonic level labels, enrichment scoring, cluster-strength calculations, pattern detection, strategy logic, or expectancy.

## Dataset
- Input reactions counted: 61.
- Yearly reaction counts: 2024=0, 2025=0, 2026=61.
- Bucket width: 0.05; bucket range: 0.00-3.35.

## 1. Where are the strongest raw ratio concentrations?
0.25-0.30 (count 7, 11.4754098361%); 0.55-0.60 (count 5, 8.19672131148%); 0.65-0.70 (count 5, 8.19672131148%); 2.00-2.05 (count 5, 8.19672131148%); 0.35-0.40 (count 4, 6.55737704918%); 1.00-1.05 (count 4, 6.55737704918%); 3.30-3.35 (count 4, 6.55737704918%); 0.30-0.35 (count 3, 4.91803278689%); 1.25-1.30 (count 3, 4.91803278689%); 1.55-1.60 (count 3, 4.91803278689%)

Strongest local peaks:
0.275 (count 7, rank 1); 0.575 (count 5, rank 2); 0.675 (count 5, rank 3); 2.025 (count 5, rank 4); 0.375 (count 4, rank 5); 1.025 (count 4, rank 6); 1.275 (count 3, rank 7); 1.575 (count 3, rank 8); 3.225 (count 3, rank 9); 1.375 (count 2, rank 10)

## 2. Do the strongest concentrations occur near common harmonic ratios?
| Reference ratio | Raw bucket | Count | Bucket is local peak? | Nearest raw peak | Within 0.05? |
| --- | --- | ---: | --- | ---: | --- |
| 0.236 | 0.20-0.25 | 0 | No | 0.275 (Δ 0.039) | Yes |
| 0.382 | 0.35-0.40 | 4 | Yes | 0.375 (Δ 0.007) | Yes |
| 0.5 | 0.50-0.55 | 0 | No | 0.575 (Δ 0.075) | No |
| 0.618 | 0.60-0.65 | 0 | No | 0.575 (Δ 0.043) | Yes |
| 0.707 | 0.70-0.75 | 2 | No | 0.675 (Δ 0.032) | Yes |
| 0.786 | 0.75-0.80 | 0 | No | 0.675 (Δ 0.111) | No |
| 1 | 1.00-1.05 | 4 | Yes | 1.025 (Δ 0.025) | Yes |
| 1.272 | 1.25-1.30 | 3 | Yes | 1.275 (Δ 0.003) | Yes |
| 1.618 | 1.60-1.65 | 0 | No | 1.575 (Δ 0.043) | Yes |

## 3. Which peaks survive all years?
None. The local export has no 2024 or 2025 reaction rows, so no raw peak can satisfy all-year survival.

## 4. Which peaks disappear?
0.275, 0.375, 0.575, 0.675, 1.025, 1.175, 1.275, 1.375, 1.575, 1.775, 2.025, 2.325, 2.775, 3.025, 3.225

## 5. Is there evidence of natural ratio clustering without harmonic-level assignment?
Yes, within the available local export there are strict local maxima in the raw-ratio histogram. However, the evidence is temporally limited because the available export is populated only in 2026; it does not validate multi-year natural clustering.

## 6. Does the data support continuing harmonic research?
Yes, but only as research. The raw histogram contains non-uniform concentrations worth auditing on a larger multi-year export. These outputs do not promote harmonic levels, create a detector, define patterns, or support strategy deployment.
