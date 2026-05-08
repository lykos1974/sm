# Experiment Log

## Stable Baseline Reference (Current Rollback Point)

Baseline profile:
- Direction: LONG only
- `breakout_context`: `POST_BREAKOUT_PULLBACK`
- `pullback_quality`: `HEALTHY`
- `active_leg_boxes`: 2
- Non-extended only
- All symbols in scope
- No early breakeven logic

Baseline metrics:
- `candidate_rows_registered`: 98
- `resolved_rows`: 89
- `win_rate_non_ambiguous`: 0.3146
- `avg_realized_r_multiple`: 0.1919
- `total_realized_r_multiple`: 17.0763
- `TP1 -> TP2 conversion`: 0.9286

Status: **Stable / profitable rollback point**.

---

## Reusable Experiment Entry Template

### Experiment ID
- ID:
- Date:
- Branch:
- Owner:

### Hypothesis (One Idea Only)
- Summary:
- Intended impact:

### Change Scope
- Files touched:
- Strategy behavior change:
- Schema change: Yes/No (if yes, justify)

### Scorecard
- `candidate_rows_registered`:
- `resolved_rows`:
- `win_rate_non_ambiguous`:
- `avg_realized_r_multiple`:
- `total_realized_r_multiple`:
- `TP1 -> TP2 conversion`:

### Decision
- Outcome: **PROMOTE / KEEP AS RESEARCH / DISCARD**
- Rationale:
- Follow-up:

---

## Historical Notes (Known Outcomes)
- Long baseline: **Stable / profitable rollback point**.
- BE at 1.5R: **DISCARD**.
- Short continuation mirror: **DISCARD**.
- Mixed long + short reversal: **KEEP AS RESEARCH**.

## Experiment ID: LATE_EXTENSION_SHADOW_V1
- ID: LATE_EXTENSION_SHADOW_V1
- Date: 2026-05-02
- Branch: current
- Owner: research

### Hypothesis (One Idea Only)
- Summary: Micro-pullback continuation entries during LONG `LATE_EXTENSION` context can produce tradeable edge.
- Intended impact: Identify whether a continuation trigger can complement the stable rollback baseline without changing live execution.

### Change Scope
- Files touched: None (shadow labeling analysis only)
- Strategy behavior change: No
- Schema change: No

### Scorecard
- `candidate_rows_registered`: 240 (`total_triggers`)
- `resolved_rows`: 240 (`valid_triggers`)
- `win_rate_non_ambiguous`: 0.0167 (`TP2_FIRST / resolved_rows`)
- `avg_realized_r_multiple`: 0.0042 (assuming `STOP_FIRST=-1R`, `TP1_FIRST=+2R`, `TP2_FIRST=+3R`)
- `total_realized_r_multiple`: 1.0 (same assumption set)
- `TP1 -> TP2 conversion`: 0.0336 (`TP2_FIRST / TP1_FIRST`)

### Decision
- Outcome: **DISCARD**
- Rationale: STOP and TP1 are nearly symmetric (`48.75%` vs `49.58%`) and TP2 incidence is too rare (`1.67%`) to support edge after costs/slippage.
- Follow-up: Optional future variants include earlier pullback entry, volatility-adjusted stop model, and momentum pre-filtering.

### Notes
- Setup definition (v1): LONG `LATE_EXTENSION`, micro pullback `O<=4`, trigger on new X breaking previous X high, entry at previous X high, stop at previous O low, fixed risk (~500), targets TP1=2R and TP2=3R.
- Method: Shadow labeling only, forward 1m candle scan, conservative ambiguity policy (same-candle stop/TP marked ambiguous).
- Outcome distribution: STOP_FIRST=117, TP1_FIRST=119, TP2_FIRST=4, AMBIGUOUS=0, NO_HIT=0.
- Additional: median candles to event ~110.

## SHORT_EXTREME_BEARISH_EXTENSION — Multi-symbol validation

Context:
Initial BTC-only test suggested strong short edge:
- filter: SHORT, BEARISH_REGIME, is_extended_move=1, active_leg_boxes>=6, LATE_EXTENSION, DEEP_GEOMETRY
- BTC-only: 16 trades, avg R +1.31, TP2 50%, stop 18.7%

Multi-symbol validation:
- symbols: BTC, ETH, SOL
- trades: 215
- avg R: -0.3757
- total R: -80.78
- TP2 rate: 12.09%
- stop rate: 73.95%
- distribution: ETH 165, SOL 34, BTC 16

Conclusion:
Rejected as general short edge.
BTC result was symbol-specific / small-sample artifact.
Do not promote to strategy module.

Important rule:
Future edge candidates must pass multi-symbol validation before implementation.

---

## SHADOW_RESEARCH_SCANNER — Event-driven structural research breakthrough

Context:
We moved shadow strategy research away from full strategy_historical_backfill runs and into a dedicated event-driven scanner.

Key performance result:
- symbols: ETH + SOL
- candles_processed: 1,902,347
- events_processed: 146,438
- event_ratio: 0.076978
- candidates_generated: 9,346
- total runtime: ~18.4 seconds

Conclusion:
The research loop moved from slow full backfill-style runs to fast event-driven structural scanning.
This is now the preferred workflow for shadow candidate discovery.

---

## BOUNCE_SHORT_REJECTION — Initial edge discovery

Candidate:
shadow_krausz_bounce_short_candidate

Observed counts:
- BTC: 0 candidates
- ETH: 0 candidates
- SOL: 2,022 candidates

Outcome on SOL:
- trades: 2,022
- avg_R: +0.5381
- stop_rate: 49.46%
- TP1_rate: 48.37%
- TP2_rate: 2.18%
- median_bars_to_event: 26

Interpretation:
This is not a generic crypto short setup.
It appears SOL-specific so far and likely represents high-beta altcoin rejection-continuation behavior after fresh bearish breakdown.

Best observed subtypes:
- bars_since_breakdown 51–100, failed_below_breakdown=0:
  - trades: 744
  - avg_R: +0.6237
  - win_rate: 53.76%
  - stop_rate: 46.24%

- bars_since_breakdown 0–25, failed_below_breakdown=0:
  - trades: 796
  - avg_R: +0.6181
  - win_rate: 52.76%
  - stop_rate: 47.24%

Important structural insight:
failed_below_breakdown=0 performed better than expected.
This suggests the edge may be a bull-trap reclaim/rejection pattern, not merely weak bounce failure below breakdown.

Decision:
Do not promote to strategy yet.
Keep as promising shadow research candidate.
Next step should be deeper bounce taxonomy / archetype classification.

---

## REVERSAL_LONG_AFTER_BREAKDOWN — Rejected current form

Candidate:
shadow_reversal_long_candidate

Counts:
- BTC: 8
- ETH: 12
- SOL: 10 in scanner comparison, 22 tested on ETH+SOL set

Outcome on tested 22 ETH+SOL reversal candidates:
- stop_count: 22
- TP1: 0
- TP2: 0
- avg_R: -1.0
- median_bars_to_stop: 1

Conclusion:
Current structural reclaim reversal logic is not tradable.
It behaves like falling-knife catching after breakdown and stops immediately.
Do not promote.

---

## Asset personality finding

Cross-asset comparison:

BTC:
- rows: 4,166
- fresh_breakdowns: 2,178
- bounce_candidates: 0
- reversal_candidates: 8

ETH:
- rows: 4,408
- fresh_breakdowns: 2,242
- bounce_candidates: 0
- reversal_candidates: 12

SOL:
- rows: 4,938
- fresh_breakdowns: 2,528
- bounce_candidates: 2,022
- reversal_candidates: 10

Interpretation:
Fresh bearish breakdowns exist across all three assets, but bounce-rejection behavior appears only in SOL so far.
This suggests symbol personality / high-beta altcoin behavior must be part of future strategy research.

Important future rule:
Do not assume one crypto strategy applies uniformly across BTC, ETH, and SOL.
Future research should classify structural archetypes by asset behavior.

---

## STRUCTURAL_PATTERN_OUTCOME_SUMMARY_V1 — Unified structural edge comparison

Context:
First unified outcome-analysis pass for canonical structural PnF patterns detected by `shadow_research_scanner.py` and analyzed with `pattern_outcome_analysis.py` across BTC, ETH, and SOL.

Scope:
- Analysis-only documentation.
- No strategy changes.
- No validation changes.
- No scanner or detector changes.
- No baseline promotion.

Key verified findings:
- `bullish_triangle` is currently the strongest verified structural continuation pattern.
  - BTC: 655 trades, avg_R +1.576, stop_rate 20.8%, tp2_rate 19.8%, median_bars_to_event 2.
  - ETH: 716 trades, avg_R +1.635, stop_rate 19.0%, tp2_rate 20.5%, median_bars_to_event 2.
  - SOL: 859 trades, avg_R +1.750, stop_rate 18.7%, tp2_rate 31.2%, median_bars_to_event 1.
- `bearish_catapult` appears to be a valid slower continuation structure.
  - BTC: 85 trades, avg_R +1.035, stop_rate 32.9%, tp2_rate 2.35%, median_bars_to_event 26.
  - ETH: 80 trades, avg_R +1.350, stop_rate 23.8%, tp2_rate 6.25%, median_bars_to_event 27.
  - SOL: 110 trades, avg_R +1.218, stop_rate 27.3%, tp2_rate 3.64%, median_bars_to_event 23.5.

Interpretation:
- Bullish triangle belongs to a compression-release family with immediate expansion behavior.
- Bearish catapult belongs to a continuation-after-trap family with slower directional persistence.
- Timing profiles differ enough that eventual execution research should treat these as separate structural families.

Decision:
- Outcome: **KEEP AS RESEARCH**.
- Rationale: Strong normalized structural findings, but no execution specialization or strategy promotion has been approved.
- Follow-up: Analyze `bearish_triangle`, `bullish_catapult`, `bullish_signal_reversal`, `bearish_signal_reversal`, and `shakeout`; then pursue overlap analysis, family classification, regime conditioning, and timing-specific execution models.

Detailed report:
- `experiments/structural_pattern_outcome_summary_v1.md`
