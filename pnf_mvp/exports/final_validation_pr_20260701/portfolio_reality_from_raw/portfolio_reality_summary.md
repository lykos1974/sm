# PnF pole portfolio reality audit

Research only. NOT PRODUCTION. NOT PROMOTED. This audit does not alter strategy code or live trading behavior.

## Fixed execution baseline

- Entry: `NEXT_COLUMN_OPEN_ENTRY`
- Stop: fixed 3-box stop
- Target: fixed 2.5R
- Management: move stop to break-even after +2R
- No TP1, TP2, trailing, scaling, or pyramiding

## Verdict: **INSUFFICIENT_DATA**

fewer than 30 resolved portfolio trades are available.

## Equity curve metrics

- `total_R`: 2.5
- `average_R_per_trade`: 2.5
- `median_R_per_trade`: 2.5
- `max_drawdown_R`: 0.0
- `max_drawdown_percent_of_peak_R`: 0.0
- `recovery_time_after_drawdown`: 
- `longest_losing_streak`: 0
- `longest_flat_BE_streak`: 0
- `longest_non_winning_streak`: 0

## Money simulation (USDT)

- `initial_capital_usdt`: 1000.0
- `fixed_position_size_usdt`: 50.0
- `final_equity_usdt`: 1125.0
- `total_pnl_usdt`: 125.0
- `max_drawdown_usdt`: 0.0
- `max_drawdown_percent`: 0.0

## Portfolio exposure

- `median_concurrent_positions`: 1.0
- `p90_concurrent_positions`: 1.0
- `max_concurrent_positions`: 1
- `average_active_risk_R`: 1.0
- `peak_active_risk_R`: 1.0
- `entries_over_2R_active_risk`: 0
- `entries_over_2R_active_risk_rate`: 0.0

## Symbol contribution

| symbol | trades | wins | losses | BE exits | total R | expectancy R | contribution % |
|---|---:|---:|---:|---:|---:|---:|---:|
| BTC | 1 | 1 | 0 | 0 | 2.5 | 2.5 | 100.0 |

## Stress flags

| flag | severity | details |
|---|---|---|
| RESEARCH_ONLY | INFO | portfolio audit only; no production strategy, live trading, or execution parameter changes |
| NO_PROMOTION | INFO | allowed verdicts never include PROMOTE |
| SYMBOL_CONCENTRATION | HIGH | BTC contributes 100.0% of total_R |
| PERIOD_CONCENTRATION | HIGH | 2026-03 contributes 100.0% of total_R |
