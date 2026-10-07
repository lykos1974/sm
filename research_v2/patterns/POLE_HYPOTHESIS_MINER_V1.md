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

### Windows operator result — 2026-10-08

The operator ran the pinned four-file preflight: 468 decisions, 431 linked
accepted trades, +49.0 gross R, and 8 conservative same-candle fill/stop
resolutions. Its report SHA-256 was
`db1d426bf42aa03419170cc927e8aa3f8da3acc41d12a5c7a14156865c993751`.
The subsequent hypothetical symmetric-cost scenarios on the same cohort were:

| Cost per side | Modeled total R | Modeled max drawdown R | Positive trades |
|---:|---:|---:|---:|
| 0 bps | +49 | 20.5 | 130/431 |
| 2 bps | +7.8944342667 | 36.6558764 | 130/431 |
| 5 bps | -53.7639143333 | 87.546138 | 130/431 |
| 10 bps | -156.5278286667 | 179.336888 | 130/431 |

The zero-total threshold under this **specific** planned-entry model is
2.384105369957962189158422217 bps per side before funding. These numbers
are operator-reported outputs of the scenario tool, not independently
inspected local artifacts or actual exchange fee/fill measurements. No
strategy selection or net profitability claim follows. The already inspected
2024 cohort is unsuitable as a fresh untouched test; the next gate is a new,
venue-matched forward cohort with audited execution costs and causal feature
provenance. Do not optimize the same 2024 trades to force a 75% win rate.

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

## Separate 2025 Binance USD-M BTC archive preflight

This is a **data acquisition gate**, not a strategy backtest or a miner input.
The 2024 outcomes have been examined repeatedly. Keep 2025 separate; earlier
unrelated January 2025 experiments mean the year must not be described as
universally untouched. Do not select rules using the 2025 results and then
call the same year a holdout.

The offline-tested `binance_um_2025_archive_preflight.py` downloads, only when
`--fetch` is explicit, twelve official monthly USD-M BTCUSDT 1m ZIPs and their
`.CHECKSUM` files. It verifies their SHA-256, sole CSV member, exact millisecond
open/close chronology, complete month coverage, finite positive OHLC and
nonnegative volume. It writes only to the operator-selected **new** research
directory. Existing ZIPs are reverified. A failed month prevents a PASS
manifest; it does not fabricate missing candles. No database, trader, strategy,
credentials or operational services are accessed. Binance archives may later
be revised; preserve the original ZIP/checksum pairs and report hashes.

From `H:\pnf screener\research_snapshots\PRZ_pole_code_20261006`, after
fast-forwarding that detached research checkout to the branch commit containing
this module, paste this one PowerShell block (new directory each time):

```powershell
$code = 'H:\pnf screener\research_snapshots\PRZ_pole_code_20261006'
$run = 'H:\pnf screener\research_snapshots\BINANCE_UM_BTC_2025_official_' + (Get-Date).ToUniversalTime().ToString('yyyyMMdd_HHmmss_fffffff')
New-Item -ItemType Directory -Path $run -ErrorAction Stop | Out-Null
Push-Location $code
try {
    python -B -m unittest -q tests.test_binance_um_2025_archive_preflight
    if ($LASTEXITCODE -ne 0) { throw 'Offline tests failed' }
    python -B -m research_v2.patterns.binance_um_2025_archive_preflight --archive-dir $run --fetch
    if ($LASTEXITCODE -ne 0) { throw "Archive verification failed; inspect $run" }
    Get-FileHash -LiteralPath (Join-Path $run 'binance_um_btc_2025_preflight.json') -Algorithm SHA256
    Write-Host "VERIFIED_ARCHIVES: $run"
} finally { Pop-Location }
```

The expected complete-year count is 525,600 consecutive 1m rows. Treat any
other count as a blocker. This is venue-matched historical OHLC, but it does
not contain tick chronology or actual fills/fees/funding. Audit execution
and produce a genuinely independent net-outcome ledger before using the
hypothesis miner. The `--fetch` run needs only access to Binance's public
data host; its archive size and download duration depend on the remote files
and connection. Data ZIPs stay local; only code, tests and protocol go to GitHub.

### Operator preflight result — 2026-10-08 Athens time

Operator reported `PASS`, 12 monthly archives and 525,600 rows, in
`H:\\pnf screener\\research_snapshots\\BINANCE_UM_BTC_2025_official_20261007_221357_1633430`.
The reported SHA-256 of `binance_um_btc_2025_preflight.json` is
`7f3e45c173e896654447030cdf152df4ca0b2e34aae2fb70237a2c8c39d3bba2`.
This is a reported local result; the raw monthly archives and manifest were
not transferred for independent inspection. Freeze that directory and do not
rerun into it. The archive integrity gate is complete. The existing causal
runner requires P&F `columns.csv` and `candles_1m.csv`, so ZIP verification
alone does not authorize a 2025 run. Next: build and audit an isolated,
versioned 2025 CSV/P&F conversion against the verified manifest; preserve
causal settings and run a single frozen comparison before considering net
execution modeling. No 2025 profitability claim is made here.

## Freeze 2025 research CSV inputs (no backtest)

`binance_um_2025_frozen_inputs.py` checks the pinned preflight manifest hash,
revalidates every monthly ZIP against that manifest, streams their 1m candles
into `candles_1m.csv`, and constructs `columns.csv` with the existing
`PnFEngine` using closed 1m close, absolute box size 100 and reversal 3. It
writes both hashes to `frozen_inputs_manifest.json` in a new sibling directory
and checks the sources again. No database or strategy runner is invoked. The
column schema matches the existing causal research runner. This reuses the
existing float-based PnF engine and does not claim tick-accurate formation.

From PowerShell, in any directory, paste this complete block after updating
the isolated code checkout to the commit containing this module:

```powershell
$code = 'H:\pnf screener\research_snapshots\PRZ_pole_code_20261006'
$source = 'H:\pnf screener\research_snapshots\BINANCE_UM_BTC_2025_official_20261007_221357_1633430'
$out = 'H:\pnf screener\research_snapshots\BINANCE_UM_BTC_2025_frozen_' + (Get-Date).ToUniversalTime().ToString('yyyyMMdd_HHmmss_fffffff')
Push-Location $code
try {
    python -B -m unittest -q tests.test_binance_um_2025_frozen_inputs tests.test_binance_um_2025_archive_preflight
    if ($LASTEXITCODE -ne 0) { throw 'Offline tests failed' }
    python -B -m research_v2.patterns.binance_um_2025_frozen_inputs `
      --archive-dir $source `
      --manifest-sha256 7f3e45c173e896654447030cdf152df4ca0b2e34aae2fb70237a2c8c39d3bba2 `
      --output-dir $out
    if ($LASTEXITCODE -ne 0) { throw 'Frozen input conversion failed' }
    Write-Host "FROZEN_INPUT_DIR: $out"
} finally { Pop-Location }
```

Check `candles=525600`, hashes, and the two CSV paths in the new folder. Stop
if anything disagrees; the module removes its own incomplete output directory
on failure. This remains data preparation. Do not infer actual net returns
from 1m candles or tune rules on 2025 and call them out-of-sample.

### Operator frozen-input result — 2026-10-08 Athens time

The operator reported successful isolated conversion in
`H:\\pnf screener\\research_snapshots\\BINANCE_UM_BTC_2025_frozen_20261007_221948_5274615`:
525,600 1m candles, 9,056 P&F columns, closed 1m close, absolute box 100,
reversal 3; candle SHA-256
`4e33b5c2a549b1eac26adfec022aab4cadce92261c13526a268f079225d16cab`;
column SHA-256
`de4a51d35b88e8666d6a0b007dd48d634b0a849ccc8c52e4773bbc23cf68b74f`.
This is operator-reported provenance, not independent inspection of local CSVs.
The source archive manifest hash was
`7f3e45c173e896654447030cdf152df4ca0b2e34aae2fb70237a2c8c39d3bba2`.
Freeze the files. The next single comparison is the existing causal LONG pole
runner with the 2024 research assumptions, 48-hour 2025 warm-up and no
parameter sweep. Report gross outcomes by quarter, opportunity count and
cost sensitivity separately. It remains unsuitable for an actual net or
untouched-test claim.

### One local 2025 causal LONG pole comparison (gross only)

The existing `backtest_workspace.create_job` and `run_job` accept a new
isolated output and pinned CSV inputs. The operator runs this only after
verifying that the current code includes the tests and two source hashes
above. Entry cohort starts 2025-01-03 00:00 UTC after 48 hours of candles;
last entry strictly before 2026-01-01 00:00 UTC; later exits may be evaluated.
The same existing portfolio assumptions as the BTC 2024 causal run apply;
no target/stop sweep, PRZ filter or newly optimized entry is enabled. The
entire annual OHLC input is read from local research CSV, not an operational
DB. The code records a new job manifest and gross research outputs; actual
fills, fees, slippage and funding are still absent. Before interpreting
results, compare settings and limitations to the 2024 frozen manifest.

```powershell
$code = 'H:\pnf screener\research_snapshots\PRZ_pole_code_20261006'
$env:BTC25_INPUT = 'H:\pnf screener\research_snapshots\BINANCE_UM_BTC_2025_frozen_20261007_221948_5274615'
$env:BTC25_OUT = 'H:\pnf screener\research_snapshots\BTC_2025_causal_single_' + (Get-Date).ToUniversalTime().ToString('yyyyMMdd_HHmmss_fffffff')
Push-Location $code
try {
    python -B -m unittest -q tests.test_backtest_workspace tests.test_pole_causal_long_research
    if ($LASTEXITCODE -ne 0) { throw 'Targeted tests failed; no run' }
    @'
import hashlib, os
from pathlib import Path
from research_v2.backtest_workspace import create_job, run_job
root = Path(os.environ['BTC25_INPUT'])
columns, candles = root / 'columns.csv', root / 'candles_1m.csv'
def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()
if sha(columns) != 'de4a51d35b88e8666d6a0b007dd48d634b0a849ccc8c52e4773bbc23cf68b74f':
    raise SystemExit('Column hash mismatch')
if sha(candles) != '4e33b5c2a549b1eac26adfec022aab4cadce92261c13526a268f079225d16cab':
    raise SystemExit('Candle hash mismatch')
output = Path(os.environ['BTC25_OUT'])
job = output.with_suffix('.job.json')
create_job(columns, candles, 1735862400000, output, job, 1767225600000)
result = run_job(job)
print('RESULT_DIR:', result['output_root'])
print('JOB_FILE:', job)
'@ | python -B -
    if ($LASTEXITCODE -ne 0) { throw 'Research comparison stopped; inspect isolated output' }
} finally { Pop-Location }
```

Only one annual comparison. Freeze the run outputs and report the manifest,
number of decisions and trades, quarterly gross R and execution limitations.
Do not select a new rule on this year then reuse it as an untouched test.

### Operator single 2025 causal comparison — 2026-10-08 Athens time

The operator reported a completed single annual run under
`H:\\pnf screener\\research_snapshots\\BTC_2025_causal_single_20261007_222234_1638708`.
The reported `causal_manifest.json` SHA-256 is
`2514a65efa27dbe3328eb747351edf814f15d851570bef9c52e5d28508709f0b8`.
The targeted workspace/causal runner tests passed (43). Operator-provided
portfolio metrics: 626 resolved trades, **-67.5 gross R**, average
-0.107827 R/trade, median -1 R, 77.5 R maximum drawdown, 19 consecutive
losses, and 29 consecutive non-wins. The decision count was not supplied.

| UTC quarter | Trades | Gross R | Win rate | Max losing streak |
|---|---:|---:|---:|---:|
| 2025 Q1 | 230 | -58.5 | 0.195652 | 19 |
| 2025 Q2 | 132 | +5.0 | 0.287879 | 10 |
| 2025 Q3 | 108 | -6.5 | 0.25 | 7 |
| 2025 Q4 | 156 | -7.5 | 0.262821 | 12 |

Quarter counts sum to 626 and gross R to -67.5. This is operator-reported
output, not independently inspected local evidence. The previously reported
BTC 2024 causal run had 431 resolved trades and +49 gross R; the 2025
single-run sign reversal invalidates a robust positive-expectancy claim for
this existing causal LONG pole setup even **before** fees, slippage and
funding. The 2025 sample has been viewed and is no longer a fresh test set.
Do not optimize the entry, stop, target, PRZ filter or cost assumption against
this loss and present the same year as out of sample. Mark the current
hypothesis **REJECT FOR PROMOTION**; preserve protected baseline unchanged.
The next research gate is an independently specified hypothesis with a new
prospective cohort and audited execution costs, not another parameter sweep
on 2025. No runtime strategy is enabled.
