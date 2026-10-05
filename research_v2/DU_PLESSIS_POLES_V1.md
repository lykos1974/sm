# `du_plessis_poles_v1` — isolated offline decision profile

Default OFF in Research Backtests. The existing `patterns/poles.py` diagnostic
labels and protected LONG pullback strategy are unchanged. Preview reads a
research candle CSV only, uses closed candle prices, produces no fills, trades,
P&L, alerts, validation records or orders. The original backtest button still
runs only `causal_long_pole`; checking this profile does not change that run.

| Source | Rule | Implementation | Test |
|---|---|---|---|
| Poles, pp. 155–159 | Prior sideways consolidation | **v1 assumption**, overlap of at least one box across the three adjacent columns immediately before breakout; not a numerical rule supplied by Du Plessis | `test_breakout_boundary_and_consolidation` |
| Poles | X/O breakout exceeds preceding same-side extreme by at least three boxes | Exact integer grid-index difference against the three-column consolidation extreme, inclusive 3; pole height is **not** a mandatory >5 filter | `test_breakout_boundary_and_consolidation` |
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

## One-event chronology audit

`python -B -m research_v2.du_plessis_poles_audit --candles <frozen research candles_1m.csv> --pole-index 28 --retrace-index 29 --box-size 100 --reversal-boxes 3`
reads only the first 10,000 closed candles. It stops on the first qualifying
28→29 decision, independently recomputes its box counts, and prints the
previous and decision candle closes, UTC times, source bounds, threshold and
status. A missing event fails; it never consults a database or infers a fill.
The operator must compare the report with the UI row and frozen input hash.

## Explicit, offline next-open simulation

The separate `du_plessis_poles_forward_sim.py` consumes at most the first
10,000 contiguous, closed 1m research candles. A candidate at candle close
may be *simulated* at the **next candle's open**, never at the theoretical
threshold. A filled SHORT/LONG receives an exit candidate only on the next
valid O→X/X→O P&F reversal at a later close; the simulated exit is the
following candle's open. Gaps fail closed, spot SHORT is blocked, and no
trade is filled without a next contiguous candle. A second candidate is
skipped while a position or pending fill exists. This next-open policy is
**our research assumption**, not a Du Plessis fill rule. The output is gross
price difference, not R or net profit: no stops, position sizing, fees,
slippage, funding, exchange fills or capital model are supplied. The app's
separate offline button and the CLI expose this simulation; the operational
trader, validation and original backtest remain untouched.

## One simulated exit audit

`python -B -m research_v2.du_plessis_poles_exit_audit --candles <frozen candles_1m.csv> --event-id du_plessis_poles_v1:EARLY_ENTRY:28:29 --box-size 100 --reversal-boxes 3`
replays at most 10,000 closed candles and checks the first post-entry P&F
reversal column independently, its close timestamp, then the very next
contiguous candle open against the separate simulator ledger. It prints only
one trace and makes no exchange or database request.
