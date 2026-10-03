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
