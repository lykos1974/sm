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

Select the frozen PnF `columns.csv`, frozen `candles.csv`, an **unused** absolute
results directory, and the earliest eligible entry candle close in Unix
milliseconds. The default `1704240000000` belongs to the earlier BTC 2024
research example; verify the intended warm-up before running. The folder picker proposes
a unique new child directory when selecting an existing results parent with
the folder picker. It checks CSV headers before creating a job or launching
the worker; `columns.csv` must contain `idx` and `candles_1m.csv` must contain
`close_time`. The verified BTC 2024 frozen input filename is `candles_1m.csv`.
It creates a `.job.json` beside the results directory and records SHA-256 hashes of both
inputs. It writes a `.run.log` beside the job. The results directory contains
`causal_manifest.json`, `causal_decisions.csv`, the portfolio outputs, and
`research_job_manifest.json` only on successful completion. Existing output
directories are never overwritten. Source files are hashed before and after
the run. A failed run may leave a partial results directory; treat it as
invalid unless the final job manifest is present and all evidence checks pass.

The job schema is `research-backtest-job-v1`. It requires `strategy_id`,
absolute paths and SHA-256 for both CSV inputs, `minimum_entry_ts`, and an
absolute new output path. Only explicitly allowlisted adapters execute.

## Verification and next gate

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
