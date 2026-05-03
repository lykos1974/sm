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
