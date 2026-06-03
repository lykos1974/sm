# PnF Strategy Research Results Log

This document tracks versioned research snapshots for strategy/validation experiments, including assumptions, run configuration, and observed outcomes.

---

## 2026-06-03 — Phase: PnF Pole Motif Execution Baseline Validation

### Research Status
- `VALIDATED_POSITIVE_RESEARCH_BASELINE / CONCENTRATED_EDGE`
- This is a validated positive research baseline, not production.
- The edge survived execution-reality, break-even management, portfolio, symbol-dependence, and market-family checks.
- Contribution concentration remains material, so this is not a fully diversified baseline.

### Fixed Research Baseline
- Entry: `NEXT_COLUMN_OPEN_ENTRY`
- Stop: fixed 3-box stop
- Target: fixed 2.5R
- Management: move stop to break-even after +2R
- No TP1, TP2, trailing, scaling, pyramiding, filters, or optimization variants

### Evidence Chain
- Execution reality audit: `EDGE_SURVIVES`
- Execution model v1 audit: `EXECUTION_READY`
- Break-even audit: `BE_IMPROVES` for break-even after +2R
- Portfolio reality audit: `PORTFOLIO_FRAGILE` due to `SYMBOL_CONCENTRATION`
- Symbol dependence audit: `EDGE_PARTIALLY_CONCENTRATED`, not BTC-dependent
- Market-family correction audit: `CONCENTRATED_EDGE`

### Key Metrics

| Metric | Value |
|---|---:|
| trades | 460 |
| wins | 332 |
| losses | 69 |
| BE exits | 59 |
| win rate | 72.17% |
| BE rate | 12.83% |
| expectancy | +1.654R/trade |
| total R | +761R |
| max drawdown | 4R |
| longest losing streak | 4 |
| median concurrent positions | 1 |
| max concurrent positions | 2 |
| average active risk | ~1.03R |
| peak active risk | 2R |

### Market-Family Result

| Market Family | Expectancy |
|---|---:|
| BINANCE | +1.655R |
| MEXC | +1.653R |

Edge was positive in both market families.

### Concentration Warning
- BTC contributes ~60.8% of total R.
- BINANCE contributes ~81.5% of total R.
- Final stability verdict remains `CONCENTRATED_EDGE`.
- Therefore this is not a fully diversified baseline.

### Artifact References
- `pnf_mvp/exports/pole_execution_model_v1_7markets_v1/execution_model_v1_summary.md`
- `pnf_mvp/exports/pole_be_research_7markets_v1/be_research_summary.md`
- `pnf_mvp/exports/pole_portfolio_reality_7markets_v1/portfolio_reality_summary.md`
- `pnf_mvp/exports/pole_symbol_dependence_7markets_v1/pole_symbol_dependence_summary.md`
- `pnf_mvp/exports/pole_baseline_stability_market_fix_7markets_v1/baseline_stability_market_fix_summary.md`

### Warnings
- **NOT PRODUCTION**
- **NOT PROMOTED**
- **RESEARCH ONLY**
- Do not activate live trading from this record.
- Do not change production strategy from this record.
- Do not optimize further based only on this snapshot.
- Next phase should address concentration risk, either by expanding the MEXC/non-BTC sample or adding concentration controls.

---

## 2026-05-01 — Phase: Fast Incremental + V4 Diagnostics + WATCH/CANDIDATE Blocking Policy

### Git / PR Context
- Fast incremental authoritative backfill mode was added and used for this run context.
- V4 diagnostics and funnel/perf export instrumentation were added prior to this run context.
- WATCH/CANDIDATE blocking policy was tested with candidate-only open-trade gate behavior in backfill registration.

### Run Command
```bash
python strategy_historical_backfill.py --settings settings.research_clean.json --symbols BINANCE_FUT:BTCUSDT --use-incremental-structure-fast --funnel-csv exports\v4_watch_unblocked.csv --perf-json exports\v4_watch_unblocked.json
```

### Validation Policy Used
- Registration eligibility: `WATCH + CANDIDATE`
- Blocking policy: candidate-only open-trade gate
- `WATCH` does **not** block `CANDIDATE`

### Key Summary (strategy_trade_export)

| Metric | Value |
|---|---:|
| total_resolved_rows | 17552 |
| win_rate_non_ambiguous | 0.5680 |
| avg_realized_r_multiple | 0.3133 |
| total_realized_r_multiple | 5498.4411 |

### Important Breakdowns

| Cohort | Trades | Avg Realized R | Total Realized R |
|---|---:|---:|---:|
| WATCH | 13448 | 0.5395 | 7255.31 |
| CANDIDATE | 4104 | -0.4281 | -1756.87 |
| LONG | 7468 | 0.5414 | 4043.19 |
| SHORT | 10084 | 0.1443 | 1455.25 |
| POST_BREAKOUT_PULLBACK | 9239 | -0.1517 | — |
| LATE_EXTENSION | 8313 | 0.8301 | 6900.30 |
| LONG_LATE_EXTENSION__BULLISH_REGIME__EXTENDED | 3011 | 1.4177 | — |
| LONG_POST_BREAKOUT_PULLBACK__BULLISH_REGIME__NON_EXTENDED | 4457 | -0.0506 | — |
| LONG CANDIDATE baseline active_leg_boxes=2 / healthy | 2226 | -0.4004 | — |

### Conclusion (Recorded)
The previously protected hypothesis **"LONG post-breakout pullback, healthy, active_leg_boxes=2, non-extended"** was **not profitable** under this validation policy/run.

The strongest observed cohort in this run is **extended / late-extension**, especially:
- `LONG_LATE_EXTENSION__BULLISH_REGIME__EXTENDED`
- `HEALTHY_GEOMETRY` with `active_leg_boxes >= 4`

### Warnings
- Do **not** change strategy based on one run alone.
- Treat this as research evidence, not final production policy.
- Next step should isolate and validate the `LATE_EXTENSION` cohort across symbols/settings.
