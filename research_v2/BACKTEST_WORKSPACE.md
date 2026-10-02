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
