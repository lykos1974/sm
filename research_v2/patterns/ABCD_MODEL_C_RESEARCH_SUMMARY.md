# AB=CD Model C Research Summary

## 1. Scope

This document records the current research findings for **AB=CD Model C** as a stable GitHub reference point before moving to trading-bot discussions.

Scope is limited to:

- **AB=CD Model C**
- **PRZ_VALID_AND_CONFIRMED_13**
- **ENTRY_RETRACE_382**
- **PRICE_MODE candle reality validation**

## 2. Artifact Chain

The main artifacts used in the current AB=CD Model C research chain are:

- `abcd_prz_convergence_local_v1`
- `abcd_d_confirmation_local_v1`
- `abcd_prz_confirmation_confluence_local_v1`
- `abcd_retest_feasibility_local_v1`
- `abcd_retest_entry_level_local_v1`
- `abcd_execution_context_v1`
- `abcd_price_mode_reality_sim_v1`
- `abcd_price_mode_trade_diagnostics_v1`
- `abcd_reaction_size_threshold_curve_v1`

## 3. Main Validated Result

Population and candle-level validation:

- **N:** 1281
- **Candle coverage:** 1281
- **Missing candles:** 0
- **Same-candle ambiguity:** 0
- **Decision event validation failures:** 0

PRICE_MODE results:

| Target | Target-first count | Population | Rate |
| --- | ---: | ---: | ---: |
| 1R | 1012 | 1281 | 0.7900078064 |
| 2R | 687 | 1281 | 0.5362997658 |
| 3R | 512 | 1281 | 0.399687744 |

## 4. Stability

Symbol-level stability:

| Symbol | 1R | 2R | 3R |
| --- | ---: | ---: | ---: |
| BTCUSDT | 0.7946859903 | 0.5362318841 | 0.4033816425 |
| ETHUSDT | 0.8047138047 | 0.531986532 | 0.4006734007 |
| SOLUSDT | 0.7789473684 | 0.5385964912 | 0.3964912281 |

Year-level stability:

| Year | 1R | 2R | 3R | Notes |
| --- | ---: | ---: | ---: | --- |
| 2024 | 0.7972027972 | 0.5314685315 | 0.3916083916 | Full research sample year |
| 2025 | 0.7838827839 | 0.5335775336 | 0.398046398 | Full research sample year |
| 2026 | Not promoted | Not promoted | Not promoted | Small sample only, n=33 |

## 5. Diagnostic Finding

The only meaningful diagnostic variable found so far is:

- `first_post_d_reaction_boxes`

Threshold curve candidate:

| Threshold | N | 1R | 2R | 3R |
| --- | ---: | ---: | ---: | ---: |
| `<=15` | 484 | 0.8615702479 | 0.5950413223 | 0.444214876 |

However, the threshold curve report final decision was:

**REACTION_SIZE_THRESHOLD_RESEARCH_NOT_SUPPORTED**

Interpretation:

- `first_post_d_reaction_boxes <= 15` is an interesting candidate filter.
- It is **not** yet validated as a production filter.
- No strategy recommendation is made from this diagnostic finding.

## 6. Research Conclusion

AB=CD Model C with `ENTRY_RETRACE_382` passed candle-level PRICE_MODE validation and deserves next-phase strategy research, but not production deployment.

## 7. Guardrails

This summary is research-only. It does not include or imply:

- Fees or slippage modeling
- Capital allocation
- Overlapping trade handling
- Live bot logic
- Production recommendation

No strategy logic is created or modified by this document.
