# `du_plessis_poles_v1` — isolated offline decision profile

Default OFF in Research Backtests. The existing `patterns/poles.py` diagnostic
labels and protected LONG pullback strategy are unchanged. Preview reads a
research candle CSV only, uses closed candle prices, produces no fills, trades,
P&L, alerts, validation records or orders. The original backtest button still
runs only `causal_long_pole`; checking this profile does not change that run.

| Source | Rule | Implementation | Test |
|---|---|---|---|
| Poles, pp. 155–159 | Prior sideways consolidation | **v1 assumption**, overlap of at least one box across the three adjacent columns immediately before breakout; not a numerical rule supplied by Du Plessis | `test_breakout_boundary_and_consolidation` |
| Poles | X/O breakout exceeds preceding same-side extreme by at least three boxes | Exact integer grid-index difference, inclusive 3; pole height is **not** a mandatory >5 filter | `test_breakout_boundary_and_consolidation` |
| Poles | Immediately adjacent opposite column retraces pole | Consecutive column IDs and X→O/O→X; pole length and retracement count include both endpoint boxes | `test_off_and_exact_half_ten_five`, `test_low_symmetry_exit_only_and_spot` |
| Trading strategy | Early action at 50%; elsewhere structural description says more than 50% | Early `2*retraced >= length`; structural diagnostic `2*retraced > length`. 10 X and 5 O passes early only | `test_off_and_exact_half_ten_five` |
| Trading strategy | HIGH weakness, LOW strength; reversal closes a short | SHORT/LONG candidates; LONG management is explicit symmetric implementation; reversal candidate only after separately acknowledged fill, never a fixed stop | `test_live_prefix_dedupe_reversal_requires_fill_and_no_auto_entry` |
| Trading strategy | Sell to close LONG differs from opening SHORT | EXIT_ONLY requires strategy-owned position; no automatic new entry; double-top/bottom confirmation remains a separate, unimplemented mode | `test_low_symmetry_exit_only_and_spot` |

Theoretical trigger is the grid level of the first eligible retracement box,
never a fill price. Decision time is the closed candle update at which that box
first exists. Fill acknowledgment requires separate evidence and is not called
by this preview. A future invalidation does not erase emitted events. Successive
opposing poles are context only, with no automatic reversal. Spot SHORT is
blocked; bearish structure may still be shown. Percentage/log grids are
rejected until their exact index mapping is supplied; a numerical price
midpoint would miscount boxes. Box size and reversal are explicit preview inputs.

The bounded preview processes at most 10,000 research candles from the start
of the selected CSV. It does not replay a full year or estimate profitability.
If source history before that prefix is absent, no prior structural context is
invented. The next research gate is an independently reviewed fill/position
adapter and a chronology check on a small frozen prefix, without operational
activation.
