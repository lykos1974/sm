# PnF Structural Pattern Research — Unified Outcome Summary v1

## Scope

This report summarizes the first unified outcome-analysis pass for canonical structural PnF patterns detected by:

- `experiments/shadow_research_scanner.py`
- `pnf_mvp/exports/core_patterns_v1.csv`
- `experiments/pattern_outcome_analysis.py`

Assets analyzed:

- `BINANCE_FUT:BTCUSDT`
- `BINANCE_FUT:ETHUSDT`
- `BINANCE_FUT:SOLUSDT`

Methodology:

- Same normalized execution logic for all patterns.
- Same TP/SL proxy model.
- Same deterministic event priority.
- Same max-bars horizon.
- No pattern-specific optimization.
- No strategy logic.
- Pure structural edge comparison.

---

## Current Verified Structural Findings

### 1) Bullish Triangle

Pattern:

- `STRICT_CONSECUTIVE_5_COL_TRIANGLE_UP_BREAK`
- Canonical compression structure.
- Visually verified through audit renderer.

#### BTC

- trades: 655
- avg_R: +1.576
- stop_rate: 20.8%
- tp2_rate: 19.8%
- median_bars_to_event: 2

Key observation:

- Immediate follow-through behavior.
- Strong continuation characteristics.
- Low delay profile.

Breakout context:

- `LATE_EXTENSION` outperformed `FRESH_BREAKOUT`.
- Suggests strong trend-continuation tendency.

#### ETH

- trades: 716
- avg_R: +1.635
- stop_rate: 19.0%
- tp2_rate: 20.5%
- median_bars_to_event: 2

Behavior:

- Nearly identical to BTC.
- Confirms cross-market consistency.

#### SOL

- trades: 859
- avg_R: +1.750
- stop_rate: 18.7%
- tp2_rate: 31.2%
- median_bars_to_event: 1

Behavior:

- Strongest triangle behavior observed.
- Extremely fast follow-through.
- Strongest continuation profile so far.

#### Unified Conclusion

Bullish triangle is currently the strongest verified structural continuation pattern.

Characteristics:

- Immediate expansion.
- Low stop rate.
- Strong TP2 conversion.
- Consistent across BTC / ETH / SOL.
- Canonical geometry.
- Visually validated.

Current interpretation:

- Compression-release continuation structure.
- Not reversal-oriented.
- Likely regime/trend dependent.

---

### 2) Bearish Catapult

Pattern:

- `STRICT_CONSECUTIVE_7_COL_BEARISH_CATAPULT`
- Canonical 7-column structure.
- Visually audited and corrected after strict consecutive-column enforcement.

Structure:

- Breakdown.
- Rebound attempt.
- Failed reclaim.
- Continuation lower.

#### BTC

- trades: 85
- avg_R: +1.035
- stop_rate: 32.9%
- tp2_rate: 2.35%
- median_bars_to_event: 26

Behavior:

- Slower continuation than triangles.
- Continuation persistence rather than explosive expansion.

#### ETH

- trades: 80
- avg_R: +1.350
- stop_rate: 23.8%
- tp2_rate: 6.25%
- median_bars_to_event: 27

Behavior:

- Stronger than BTC.
- Cleaner continuation profile.

#### SOL

- trades: 110
- avg_R: +1.218
- stop_rate: 27.3%
- tp2_rate: 3.64%
- median_bars_to_event: 23.5

Important:

- Earlier negative SOL result was likely caused by previous proxy methodology.
- Unified analyzer now shows positive edge.

#### Unified Conclusion

Bearish catapult appears to be a valid continuation structure with:

- Slower timing profile.
- Lower explosive payoff behavior than triangles.
- Stronger directional persistence.
- Significantly longer holding profile.

Current interpretation:

- Failed rebound continuation.
- Continuation-after-trap structure.
- Likely trend-continuation family.

---

## Important Cross-Pattern Observation

Timing behavior differs dramatically.

Bullish triangle:

- Median event: 1–2 bars.
- Immediate expansion.
- Explosive follow-through.

Bearish catapult:

- Median event: 23–27 bars.
- Slower continuation.
- Delayed resolution.

This strongly suggests:

- Different structural families.
- Different execution styles eventually required.
- Different holding-time expectations.
- Different volatility behavior.

---

## Current Structural Family Hypothesis

### Compression Family

Examples:

- `bullish_triangle`
- `bearish_triangle`

Likely behavior:

- Immediate expansion.
- Breakout acceleration.
- Volatility release.

### Continuation Family

Examples:

- `bullish_catapult`
- `bearish_catapult`

Likely behavior:

- Trend persistence.
- Failed retracement continuation.
- Slower directional continuation.

### Reversal / Trap Family

Examples:

- `shakeout`
- Signal reversal patterns.

Research status:

- Not yet outcome-analyzed.
- Visually verified.
- Semantics appear correct.

---

## Structural Layer Status

The project now has:

- Canonical structural detectors.
- Strict consecutive-column semantics.
- Visual audit tooling.
- Normalized event datasets.
- Unified outcome analysis.
- Cross-pattern comparability.

Current workflow:

```text
Detector
→ Visual verification
→ Structural dataset
→ Unified outcome analysis
→ Cross-pattern comparison
→ Context conditioning
→ Future execution research
```

---

## Most Important Current Conclusion

The project is no longer operating at generic breakout signal level.

It is now operating at canonical structural PnF research level.

Patterns now exhibit:

- Distinct geometry.
- Distinct timing behavior.
- Distinct continuation/reversal semantics.
- Distinct payoff distributions.
- Cross-market consistency.

This is the point where genuine structural trading research begins.

---

## Recommended Next Research Priority

Next patterns to analyze using the unified analyzer:

1. `bearish_triangle`
2. `bullish_catapult`
3. `bullish_signal_reversal`
4. `bearish_signal_reversal`
5. `shakeout`

Then:

- Overlap analysis.
- Family classification.
- Regime conditioning.
- Execution specialization.
- Timing-specific execution models.

---

## Governance Note

This summary is research documentation only. It does not promote any pattern to strategy, alter validation behavior, or change the stable rollback baseline.
