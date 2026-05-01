# PnF Strategy Research Results Log

This document tracks versioned research snapshots for strategy/validation experiments, including assumptions, run configuration, and observed outcomes.

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
