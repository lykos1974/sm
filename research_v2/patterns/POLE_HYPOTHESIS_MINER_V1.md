# Bounded pole hypothesis miner v1

Research only. This is the first executable stage of
`pole_genetic_hypothesis_miner_design.md`, using a fixed set of 21 simple
rules instead of an LLM or genetic search. It does not produce signals,
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
alone. Do not feed it a gross-only pole ledger, a retrospective completed
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

## Run after input audit

```powershell
python -B -m research_v2.patterns.pole_hypothesis_miner `
  --opportunities 'H:\research\audited_independent_opportunities.csv' `
  --output 'H:\research\new_miner_report.json'
```

The output path must be new. To verify the offline module:

```powershell
python -B -m unittest -q tests.test_pole_hypothesis_miner
python -B -m py_compile research_v2\patterns\pole_hypothesis_miner.py tests\test_pole_hypothesis_miner.py
```
