# PnF Forensic Audit Handoff

Updated: 2026-09-21  
Repository: `lykos1974/sm`  
Branch: `feature/binance-microstructure-collector`  
Pull request: `#350`

## Operating state

- Crypto collector and persisted-state scanner may continue running.
- Strategy validation and alerts must remain **OFF**.
- The ideal-entry observer is observation-only; it never sends orders.
- Do not reset or delete any user database.
- Do not ask the user to install intermediate commits. Prepare one consolidated update.

## Protected behavior

- Do not change entry, stop, TP, RR, or setup promotion rules without explicit approval.
- Preserve the protected long-only baseline.
- `pnf_engine.py`: PnF construction and basic signals only.
- `structure_engine.py`: structural truth only.
- `strategy_engine.py`: setup classification only.
- UI/scanner code must not make strategy decisions.
- Operational alerts remain one per column, close-confirmed, double-top/double-bottom only.

## GitHub fixes already published

| Remote commit | Change | Verification |
|---|---|---|
| `17a25ab0` | Causal three-candle observer expiry and legacy-window migration | Observer audit passed locally on Windows |
| `c4f2628` | Canonical atomic checkpoint, monotonic watermark guard, consistent checkpoint read | Manual round-trip/stale-write tests passed |
| `58304ed` | Live/historical eligibility alignment; `REJECT` excluded; closed final historical candle retained | 6 targeted tests passed |
| `5983dd8` | Detached PnF signal snapshots and causality/immutability tests | 3 targeted tests passed |
| `8181a7d` | Activation-candle outcome, exact three-candle pending expiry, causal per-candle batch validation | 15 targeted validation/regression checks passed; remote tree verified byte-for-byte |

Remote branch head expected before this handoff update: `8181a7decb6ac81770254905af113dcc8bb7f4dc`.

## Completed safe stage: validation chronology

Commit `8181a7d` completed the prior diagnostics-first stage:

- Activation candles are now evaluated for stop/target outcomes.
- Unactivated pending setups persist progress and expire after exactly three eligible candles.
- Multi-candle refresh validation now preserves causal `update -> evaluate/register` ordering and matches one-candle processing.
- BE+TP2 same-candle behavior was locked by a diagnostic test only; its optimistic TP2-first policy was not changed.
- Strategy parameters, entry/SL/TP/RR, promotion rules, alerts, and the protected long-only baseline were unchanged.
- Validation and alerts remain OFF.

## Confirmed critical findings still open

1. `pnf_mvp/strategy_validation.py::_resolve_long_after_tp1` and `_resolve_short_after_tp1`
   - If BE and TP2 occur in the same candle, TP2 wins optimistically.
   - Required rule: conservative/ambiguous handling must be explicitly approved before changing behavior.

2. `pnf_mvp/strategy_validation.py::_should_activate`
   - Activation is based on candle close, not limit touch/trade-through.
   - Impact: this path is not equivalent to the stated execution model.

3. `pnf_mvp/app.py::_refresh_incremental_once`
   - Checkpoint persistence occurs before observer/validation completion.
   - Impact: a downstream failure can advance the watermark and permanently skip work on retry.

## High-risk reproducibility gaps

- Clean GitHub checkout previously lacked `Storage.save_checkpoint`; fixed by `c4f2628`, but Windows installation has not yet been consolidated.
- Local Windows files were previously modified by a hash-locked installer and may differ from GitHub.
- Claimed modules `strategy_setup_generation.py` and `strategy_setup_generation_incremental.py` are absent from this branch.
- Full XAUUSD figures (`142` opportunities, `112` resolved, `+169.5645R`, DD `2R`) have not been independently reproduced from a clean checkout and frozen manifest.
- Existing historical/live shadow comparisons can share the same underlying PnF/strategy bug and are not independent proof of causality.

## Confirmed causal components

- Completed PnF columns were not observed to mutate after reversal.
- PnF signal checks use the current close update; no future column is referenced.
- `structure_engine.py` derives completed swing levels from `columns[:-1]`; no retrospective pivot timestamp reassignment was found in the inspected path.
- These findings do not validate the execution engine or reported trading metrics.

## Test limitations

- The `8181a7d` stage passed 15 targeted validation/regression checks; earlier targeted `unittest` and manual persistence checks passed as listed above.
- The audit runtime lacks `pytest` and `pyarrow`; therefore no claim of a clean full-suite run has been made.

## Next smallest safe stage

Diagnostics/tests first; do not change execution policy:

1. Independently review commit `8181a7d` for chronology regressions and protected-behavior risk.
2. Add a focused regression test proving checkpoint ordering and retry behavior when observer or validation fails.
3. Correct checkpoint ordering only if the test independently demonstrates watermark advancement before downstream completion.
4. Keep BE+TP2 same-candle behavior unchanged and report it as an unresolved execution-policy choice.
5. Run targeted tests and publish the verified consolidated result to GitHub.
6. Only then prepare one Windows update/install procedure.

## Recommended model routing

- Use `gpt-5.6-sol` with medium effort for code inspection, tests, implementation, and GitHub work.
- Use Astra with medium effort only for final judgment on methodology or result-changing execution rules.
- When delegating, use a narrow task with no inherited full conversation and require a concise report.

## Prompt for the next chat

> Read `AUDIT_HANDOFF.md` from branch `feature/binance-microstructure-collector` in `lykos1974/sm`. Continue only the “Next smallest safe stage”. Use diagnostics-first, do not change strategy parameters or the protected long-only baseline, keep validation and alerts OFF, and do not ask me to install anything until one consolidated package is verified. Keep updates concise and maintain GitHub.
