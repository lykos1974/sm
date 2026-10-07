# Bounded pole hypothesis miner v1

Research only. This is a separate chronological split checker using a fixed
set of 21 simple rules instead of an LLM or genetic search. An earlier
executable `pole_p2_candidate_sampler.py` already enumerates P+2 rule
intersections over seven symbols. Its 4,023-trade `UNIVERSE_MATCH` and
+251.5R are historical **gross simulation** evidence, not the audited net-R
input required here; its ranked candidates have already been exposed to that
history. This checker does not produce signals,
simulate fills, connect to an exchange, or change the protected baseline.

## Input contract

The operator must first produce an independently audited, chronological CSV
containing **one independent executable opportunity per row**, with this
exact header and order:

```text
opportunity_id,decision_ts,known_at,symbol,side,pole_boxes,reversal_boxes,retrace_ratio,relative_pole_size,net_r
```

`decision_ts` and `known_at` are UTC milliseconds. `known_at` is the latest
availability time of **every** selection feature in the row; it must be no
later than `decision_ts`. `net_r` is the validated, realized result after
fees, slippage, funding where applicable, and the actual execution policy.
The miner cannot prove the origin or completeness of these values from CSV
alone. The older P+2 candidate sampler manifest does not record all source
paths/hashes or a cost model, and its results CSV is an aggregate candidate
table, not one row per independent net trade. Do not feed it that table, a
gross-only pole ledger, a retrospective completed
column, duplicated opportunities, an operational DB, or a current-year result
already inspected during rule development. Preparing this audited input is
the next gate; no such annual net-R dataset has been verified in this stage.

Only LONG/SHORT, exact integer box counts, exact finite Decimal retrace and
net R, and the predefined relative-size categories SMALL/NEAR/LARGE/EXTREME
are accepted. Extra columns, including future outcomes or excursions, fail.
Input SHA-256 and split boundaries are written to the new report.

## Search and interpretation

The 21 candidate IDs and thresholds are frozen in code. The first 60% of
chronological opportunities are train, the next 20% validation, and the last
20% test. Only train and validation determine the finalist; test is scored
once afterward. Minimum selected observations are 100 train and 50
validation. The input universe itself needs at least 100/50/50. The report
shows all attempted candidates, including those failing sample gates.

This small search is an initial diagnostic, not a profitability certificate.
The test score of the selected rule has selection uncertainty, temporal
dependence, and potentially overlapping opportunities. Independent walk-
forward cohorts, multiple-testing controls, cost/fill provenance and forward
observations are required before any strategy claim. Never tune thresholds
against the printed test score or interpret a high win rate without net R
and drawdown. The code does not automatically promote any candidate.

## Frozen BTC 2024 provenance preflight

`pole_miner_btc_preflight.py` checks the four operator-reported SHA-256 values
for the frozen causal decisions, causal manifest, accepted portfolio trades,
and portfolio manifest. It checks the 468 decision IDs against the 431 accepted
OPP IDs, decision/entry/exit chronology, duplicate ownership, LONG/BTC
identity, and gross-R reconciliation. It reads only those four files and
writes one new report. It does **not** produce miner input: this run is gross
OHLC evidence and its 2024 results have already been inspected. The preflight
does not infer exchange fills, costs, or a fresh out-of-sample cohort.
The portfolio simulator can record entry and exit at the same closed-candle
timestamp for `SAME_CANDLE_FILL_STOP_CONSERVATIVE`, with exactly -1R. This is
the sole permitted equality in the preflight; other equal or reversed times
remain invalid. The report counts these conservative same-candle stops.

From the repository root on the operator's Windows PC:

```powershell
$run = 'H:\pnf screener\research_snapshots\BTC_causal_2024_20261001_075402_5906621'
python -B -m research_v2.patterns.pole_miner_btc_preflight `
  --decisions (Join-Path $run 'causal_decisions.csv') `
  --causal (Join-Path $run 'causal_manifest.json') `
  --trades (Join-Path $run 'portfolio\portfolio_reality_trade_sequence.csv') `
  --portfolio (Join-Path $run 'portfolio\portfolio_reality_manifest.json') `
  --output (Join-Path $run 'pole_miner_preflight.json')
```

Only use a new output path. If hashes or relationships disagree, stop before
any cost model or hypothesis search. A separate audited cost model and a new
forward period are required for a net-performance claim.

## Illustrative cost sensitivity after PASS

`pole_miner_btc_cost_scenario.py` reads the same pinned decision/trade bytes
plus a successful preflight. It reconciles the accepted cohort again, then
reports 0, 2, 5 and 10 basis points **per side** using exact Decimal math:
`modeled_R = gross_R - 2 * bps/10000 * planned_entry / abs(planned_entry-stop)`.
The entry/stop are theoretical decision levels, not exchange fill prices.
The model assumes symmetric notional at entry and exit and excludes funding;
its result is an illustrative sensitivity, never audited net profitability.
It cannot establish an untouched 2024 test set or supply `net_r` to the miner.

```powershell
$run = 'H:\pnf screener\research_snapshots\BTC_causal_2024_20261001_075402_5906621'
python -B -m research_v2.patterns.pole_miner_btc_cost_scenario `
  --decisions (Join-Path $run 'causal_decisions.csv') `
  --trades (Join-Path $run 'portfolio\portfolio_reality_trade_sequence.csv') `
  --preflight (Join-Path $run 'pole_miner_preflight.json') `
  --output (Join-Path $run 'pole_miner_cost_scenario.json')
```

## Run after input audit

```powershell
python -B -m research_v2.patterns.pole_hypothesis_miner `
  --opportunities 'H:\research\audited_independent_opportunities.csv' `
  --output 'H:\research\new_miner_report.json'
```

The output path must be new. To verify the offline module:

```powershell
python -B -m unittest -q tests.test_pole_hypothesis_miner tests.test_pole_miner_btc_preflight tests.test_pole_miner_btc_cost_scenario
python -B -m py_compile research_v2\patterns\pole_hypothesis_miner.py tests\test_pole_hypothesis_miner.py research_v2\patterns\pole_miner_btc_preflight.py tests\test_pole_miner_btc_preflight.py research_v2\patterns\pole_miner_btc_cost_scenario.py tests\test_pole_miner_btc_cost_scenario.py
```
