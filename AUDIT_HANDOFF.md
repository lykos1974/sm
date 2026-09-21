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

Remote branch head expected: `5983dd8d8c3f384909aa810f3764416b4a96248c`.

## Confirmed critical findings still open

1. `pnf_mvp/strategy_validation.py::update_pending_with_candle`
   - The activation candle is not evaluated for stop/target after activation.
   - Impact: an immediate loss or ambiguous candle can survive and later become a win.

2. `pnf_mvp/strategy_validation.py::update_pending_with_candle`
   - Pending rows do not implement the declared three-candle expiry; `bars_observed` is not persisted while unactivated.
   - Impact: orders can activate after their legal lifetime.

3. `pnf_mvp/strategy_validation.py::_resolve_long_after_tp1` and `_resolve_short_after_tp1`
   - If BE and TP2 occur in the same candle, TP2 wins optimistically.
   - Required rule: conservative/ambiguous handling must be specified and tested before changing production behavior.

4. `pnf_mvp/strategy_validation.py::_should_activate`
   - Activation is based on candle close, not limit touch/trade-through.
   - Impact: this path is not equivalent to the stated execution model.

5. `pnf_mvp/app.py::_refresh_incremental_once` and `_run_validation_for_symbol`
   - A batch updates the PnF engine through all new candles, replays pending outcomes, then evaluates setups only at the final state.
   - Impact: intermediate opportunities are missing and a final setup is registered after its possible lifetime candles were already replayed.

6. `pnf_mvp/app.py::_refresh_incremental_once`
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

- Targeted `unittest` and manual persistence checks passed as listed above.
- The audit runtime lacks `pytest` and `pyarrow`; therefore no claim of a clean full-suite run has been made.

## Next smallest safe stage

Diagnostics/tests first; do not immediately rewrite validation:

1. Add minimal regression tests for activation-candle outcome, three-candle expiry, BE+TP2 same candle, and one-candle-versus-batch equivalence.
2. Demonstrate each current failure independently.
3. Separate pure chronology corrections from execution-model choices.
4. Apply only chronology corrections whose expected behavior is already explicit.
5. Run targeted tests and publish one consolidated GitHub commit.
6. Only then provide a single Windows update/install procedure.

## Recommended model routing

- Use `gpt-5.6-sol` with medium effort for code inspection, tests, implementation, and GitHub work.
- Use Astra with medium effort only for final judgment on methodology or result-changing execution rules.
- When delegating, use a narrow task with no inherited full conversation and require a concise report.

## Prompt for the next chat

> Read `AUDIT_HANDOFF.md` from branch `feature/binance-microstructure-collector` in `lykos1974/sm`. Continue only the “Next smallest safe stage”. Use diagnostics-first, do not change strategy parameters or the protected long-only baseline, keep validation and alerts OFF, and do not ask me to install anything until one consolidated package is verified. Keep updates concise and maintain GitHub.
