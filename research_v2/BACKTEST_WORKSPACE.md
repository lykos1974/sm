# Research Backtest Workspace — first isolated stage

## Decision

The operational Tkinter scanner remains unchanged. A standalone research
window (`python -B -m research_v2.backtest_app`) launches the same offline job
runner available from CLI (`python -B -m research_v2.backtest_workspace --job
<absolute-job-json>`). It does not import the scanner, collector, validation
service, alerts, or trader. Results go to a new research directory; the worker
does not open an operational SQLite database. The GUI does not install or start
any live service.

The first reviewed dispatch is `causal_long_pole` using frozen BTCUSDT CSV
inputs and the existing pole portfolio simulator. Its result is historical,
gross R from 1m OHLC. It does not establish net profitability or exchange
fills. Other patterns are not runnable merely because they appear in an
inventory. Each needs causal signal evidence, an entry/exit specification,
an execution adapter, and comparison gates.

## Local use

Run from the repository root:

```powershell
python -B -m research_v2.backtest_app
```

In the window, select only the frozen BTC 2024 `results` folder and click
"Run backtest". The application finds `columns.csv` and `candles_1m.csv`,
validates their CSV roles and exact audited SHA-256 values, and uses the frozen
warm-up boundary `1704240000000`. It creates a unique research output folder
under `research_snapshots` with a `.job.json` and `.run.log` next to it. It
never overwrites the earlier failed attempt. The results directory contains
`causal_manifest.json`, `causal_decisions.csv`, the portfolio outputs, and
`research_job_manifest.json` only on successful completion. Existing output
directories are never overwritten. Source files are hashed before and after
the run. A failed run may leave a partial results directory; treat it as
invalid unless the final job manifest is present and all evidence checks pass.

"View trades from a previous run" opens a completed results directory and
plots each trade over the frozen 1m candles, with selectable windows around
entry or exit. The signal's entry, stop, and 2.5R target appear as horizontal
levels. The trade ledger records exit time and R, but no exchange exit fill
price; the chart marks exit **time only** and never guesses the fill price.
The previous/next arrow buttons traverse the ledger in its recorded order,
show the current position, and stop at the first/last trade. The dropdown
remains available for jumping to a specific ID.
The viewer checks the job and causal manifests, decision ledger hash, and
frozen input hashes. It does not run a backtest or alter the source files.

The same viewer offers **Candles / PnF** and entry/exit focus. PnF renders
the pinned `columns.csv` with the frozen box size, X/O boxes, and trade entry
and exit time markers. Entry is marked at its simulated level; the ledger has
no exit fill price, so the exit has a time marker only. Event-to-column mapping
uses the latest column start at or before the event. Completed PnF columns
are retrospective: their final boxes may have formed after the marked event;
do not read them as the live PnF state at entry or exit. This chart is for
inspection and does not change strategy decisions or results.

A read-only table beside either chart explains each selected trade. It derives
the LOW_POLE LONG O/X/O motif, pole size, breakout excess, retrace ratio, and
signal knowledge time from the pinned PnF columns and causal decision ledger.
It separately identifies the eligible candle, simulated three-candle limit
fill, stop/target/BE policy, and later exit result. If the ledger disagrees
with the frozen columns or the 1m candle touched by the simulated fill, the
viewer fails closed. Exit fill price is unavailable; the table does not infer
one. This explanation is historical gross OHLC research, not a live-order or
net-profitability claim.

The panel now verifies the bounded fill-to-exit 1m candle sequence against
the existing simulator's BE/stop/target classifier. It shows the 2R trigger
level, the candle that armed BE (when applicable), the later exit candle,
its high/low, and the reason for stop/target/BE resolution. A same-candle
OHLC ordering ambiguity or a trade ledger contradiction fails closed. These
times are candle-close timestamps; no intraminute fill order or exchange
execution price is claimed. No historical run or strategy result is changed.

The job schema is `research-backtest-job-v1`. It requires `strategy_id`,
absolute paths and SHA-256 for both CSV inputs, `minimum_entry_ts`, and an
absolute new output path. Only explicitly allowlisted adapters execute.

## Verification and next gate

### Windows operator run — 2026-10-02

The operator selected the frozen BTC 2024 `results` directory in the simplified
window. The completion screenshot reported 468 causal decisions, 431 resolved
portfolio trades, and +49.0 gross R, matching the three previously recorded
reference totals. The displayed output directory was
`H:\pnf screener\research_snapshots\BTC_2024_backtest_20261002_101157_613242_7f1141f7`.
This is screenshot evidence of summary parity. The result files and their
hashes have not been independently inspected here; gross R does not include
exchange fills, fees, slippage, or funding.

Next gate: independently inspect the local job manifest, causal manifest,
trade ledger, input hashes, and output integrity before treating the Windows
run as an audited reference. Keep the operational scanner and protected
baseline outside this research stage.

Targeted synthetic offline tests:

```powershell
python -B -m unittest -q tests.test_backtest_workspace tests.test_pole_causal_long_research
python -B -m py_compile research_v2/backtest_workspace.py research_v2/backtest_app.py
```

Before Windows use, independently review the branch and verify parity on the
frozen BTC 2024 reference inputs: 527,040 candles, 468 causal decisions,
431 resolved portfolio trades, +49 gross R under the prior execution model.
These are reference observations, not a promised result from unverified paths.
Inspect the new manifest and output hashes. Audit the operational scanner
integration separately before adding a Research tab. Extend the strategy
catalog as a versioned execution registry only after each adapter passes
causality, restart, execution, and cost checks. The protected baseline and
all runtime services remain untouched.

## Selected UTC entry period

The desktop app accepts inclusive UTC dates from 2024-01-03 through
2024-12-31; the default covers the full eligible BTC 2024 period. Dates
select the eligibility time of **new causal decisions**. The same pinned
`columns.csv` and `candles_1m.csv` supply the complete prior PnF context
and later candles for pending fills and exits. A selected signal can exit
after the chosen end date. This is an entry cohort, not calendar P&L.
Each period starts without a simulated open position. Results from separate
periods need not sum to the full-year portfolio because the one-position
constraint differs at period boundaries. The job and result manifests record
the exclusive end timestamp and policy. Old v1 jobs remain readable; jobs
with an end boundary use v2. The target remains 2.5R and the existing BE
trigger remains 2R. Results are gross OHLC research.

The earlier `research_v2/patterns/pole_core_motif_r_targets.py` explored
fixed-stop excursions at 1, 1.25, 1.5, 2, 2.5, 3 and 4R. It used labeled
column paths and a stop-first approximation when event ordering was
unavailable. It was **not** a causal executable re-simulation of this BTC
2024 portfolio; its conclusions cannot establish that 3R or 4R improves
net performance here. A future target comparison needs independent pinned
replays with identical decisions, entry/stop rules, costs and intrabar
uncertainty. This change does not alter the target.

## Target sweep to 10R

The optional checkbox performs independent portfolio replays for 2.5R,
3R, 4R, 5R, 6R, 7R, 8R, 9R and 10R. It reuses the **same selected causal
decisions and pinned CSVs**, a three-candle pending limit, three-box stop,
and BE trigger at 2R. The 2.5R replay remains the ordinary chartable run.
Each other target has its own isolated `target_sweep/target_*R/` portfolio
ledger and manifest. Different exit times can change later one-position
admission; comparing only target touches on the original 2.5R trades would
miss that effect.

`target_sweep/comparison.csv` lists each variant's resolved trades, target,
stop and BE exits (including conservative same-candle fill/stop as its own
resolved -1R category), gross R, mean R, drawdown, losing streak, and the hash of
its underlying portfolio manifest. The adjacent manifest pins the target
grid, input hashes, period, BE rule and comparison-file hash. The research
job uses schema v3; v1 and v2 jobs remain readable. Running all variants
can take substantially longer than the single-target backtest. These are
in-sample gross 1m OHLC results without exchange fills, fees, slippage or
funding. No target is selected automatically or promoted to live trading.

The **Συγκριτικό chart Profit / Drawdown** button opens an existing completed
target-sweep run without recalculation. It reads the SHA-checked comparison
and matching research job manifest. Blue profit bars extend above zero and
red maximum-drawdown bars below zero, both on the same R scale. The chart
shows only these two result measures for each target. If the CSV, manifest,
target grid or completed-job binding is changed or missing, it refuses to
render. The operator can choose the result folder after restarting the app.

The same window also offers **Εξέλιξη μπάνκας**. Tick any combination of
targets R to overlay their exit-ordered curves with distinct colors; **Όλα**
checks all nine targets. At least one target remains selected. The curves use a
shared UTC time/USDT scale. Enter an explicit starting balance and fixed USDT
risk per trade (the displayed 1000/10 values are editable examples). The
research visualization computes `balance = initial + fixed_risk * cumulative_R`,
without compounding. It checks each portfolio manifest against the comparison
hash and reconciles the recorded trade count, cumulative R and drawdown before
drawing. It reads the completed sweep without another backtest or any database
access. This hypothetical gross balance excludes fees, slippage, funding,
exchange fills and capital/margin constraints; it is not an account statement.
Both chart views resize with the window, including when maximized; the axes,
target bars, curves and legend are redrawn from the same loaded research data.

## One bounded cost stress on a completed target sweep

From the repository root run `python -B -m research_v2.target_cost_gate` and
select the completed BTC target-sweep **results** folder. The command reads the
existing pinned job, comparison manifest, causal decisions and each portfolio
ledger. It creates only `results/bounded_cost_gate/` with a CSV and hash
manifest, refusing an existing folder. It does not replay candles, change a
strategy parameter, touch SQLite, or contact an exchange. For scripted use,
`--result-dir` and optional `--output-dir` are accepted.

The nine existing targets each receive the same three illustrative all-in
cost stresses: 0, 10 and 20 basis points **per side**. The stop distance and
entry price determine the approximate notional per 1R risk unit; both entry
and exit are charged on that *entry notional proxy*. The report lists gross R,
proxy cost R, adjusted R, exit-to-exit drawdown and the worst trading quarter.
Zero-cost gross R and drawdown must reconcile with the original manifests.
The exit fill price, true notional, real spread, slippage, fees, funding,
margin and intratrade drawdown are unknown. The adjusted values are a
sensitivity check, **not verified net profitability or a target recommendation**.
Do not select the 2024 target with the highest adjusted R as though it were
out-of-sample evidence. A later, separate unseen-period test would be needed
before considering any target for execution.

## MEXC live-money readiness — 2026-10-03

An operator proposed 4 USDT for live MEXC BTC_USDT Futures. **Do not activate
orders from this research stage.** The BTC 2024 causal pole run uses Binance
USD-M 1m OHLC and gross simulated fills. The research job cannot consume live
MEXC candles or submit, reconcile, stop, or close exchange orders. The target
sweep is in-sample and provides no MEXC net expectancy.

MEXC's public BTC_USDT trading rules displayed a minimum contract size of
0.0001 BTC (approximately 8.5 USDT notional at the inspected price), exceeding
4 USDT without leverage. MEXC's June 2026 API futures fee notice lists
maker 0.06% and taker 0.08%, independently of promotional UI rates. For one
minimum contract and the existing three-box, 100 USDT/box stop, the indicative
price risk is 0.03 USDT; a taker entry plus taker exit near 8.5 USDT notional
adds about 0.0136 USDT, before slippage, funding, liquidation, or fee changes.
This is about 0.0436 USDT, over 1% of a 4 USDT balance. These are illustrative
public-rule calculations, not account-specific tradability or a guarantee.

At the same illustrative 85,000 USDT BTC price and fixed 300 USDT stop,
two 0.08% taker fees consume about 0.453R per minimum-contract trade.
The existing BTC 2024 sweep reports gross 49R/431 trades (0.114R/trade)
at 2.5R, and gross 101R/405 trades (0.249R/trade) at 10R. Applying these
*current, fixed-price* fees as a rough sensitivity gives respectively
1.47 - 5.86 = -4.39 USDT and 3.03 - 5.51 = -2.48 USDT, before slippage
or funding. This is **not** an exact historical net backtest: 2024 prices,
venue, fees and fills differ. It demonstrates a decisive cost gate that
leverage cannot remove, because fixed-contract P&L and fees are unchanged
by the margin multiple. No target from the in-sample sweep is live-approved.

Sources (rules and fees must be rechecked at execution time):

- https://www.mexc.com/futures/BTC_USDT
- https://www.mexc.com/announcements/article/updates-to-api-futures-trading-fees-jun-1-2026-17827791535742

Zero-fee check (2026-10-03): MEXC's 0-Fee Fest announced 0% maker and taker
for certain *website/app* Futures pairs, including QNTUSDT and ARBUSDT/
ARBUSDC, conditional on account and region. Their announcements explicitly
exclude API trading. The separate published Futures API schedule overrides
promotional 0-fee rates; no Futures pair with verified 0% maker **and** taker
fees for this automated API workflow was identified. Confirm the actual
account-specific API rates and eligibility before any cost model, and never
substitute a promotional UI quote for an API execution fee. Even a genuine
0-fee pair would need fresh venue-specific causal strategy evidence.

- https://www.mexc.com/announcements/article/0-fee-fest-17827791538872
- https://www.mexc.com/announcements/article/0-fee-fest-17827791538957

The next separate gate is an audited MEXC-data causal forward observer and
account-specific minimum-order, fee, margin, and liquidation preflight. Exact
signal chronology, sizing, exchange-confirmed entry/exit, protective stops,
restart reconciliation, and maximum-loss halt require independent verification
before any live-order capability. No credential or order access is part of this
research application.
