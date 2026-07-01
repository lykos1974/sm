# Final validation PR — +761R executable sizing check

## Verdict

The regenerated pipeline in this repository **does not prove the documented +761R strategy is fully executable**, because the raw input universe available in this checkout contains only five Binance symbols (`BTCUSDT`, `ETHUSDT`, `BNBUSDT`, `SOLUSDT`, `XRPUSDT`) and regenerates only **1** resolved portfolio trade, not the documented 460-trade / +761R seven-market research baseline.

It does prove that the currently available raw-input pipeline can emit executable sizing fields for all regenerated trades.

## Raw-input pipeline executed

- Source database: `pnf_mvp/pnf_mvp.db`
- Settings/profile source: `pnf_mvp/settings.research_clean.json`
- Generated columns: `pnf_mvp/exports/final_validation_pr_20260701/columns/`
- Generated pole labels: `pnf_mvp/exports/final_validation_pr_20260701/poles/`
- Portfolio output: `pnf_mvp/exports/final_validation_pr_20260701/portfolio_reality_from_raw/`
- Money simulation: existing `research_v2.patterns.pole_portfolio_reality_audit` money layer with `initial_capital_usdt=1000.0` and `fixed_risk_usdt=50.0`.

## Final report metrics

| Metric | Value |
|---|---:|
| Total R | 2.5 |
| Final equity | 1125.0 USDT |
| Max DD | 0.0 USDT / 0.0R |
| Profit factor | undefined_no_losses |
| Sizing available | true |
| Number of trades missing sizing | 0 |
| Regenerated resolved portfolio trades | 1 |

## Sizing formula verification

Formula verified for every regenerated trade:

```text
position_qty = fixed_risk_usdt / abs(entry_price - stop_price)
```

Formula violations: 0.

## Trades missing sizing

None.

## Assumptions and limitations

- No strategy logic was changed.
- No optimization was run.
- Production code was not modified.
- Fixed money risk was treated as one R: `fixed_risk_usdt=50.0`.
- Approximate notional is computed as `entry_price * position_qty`.
- Profit factor is reported as `undefined_no_losses` when there are gross wins and no gross losses.
- The +761R claim cannot be re-proven from this checkout alone because the raw data required to regenerate the documented seven-market 460-trade baseline is not present in `pnf_mvp/pnf_mvp.db`.
