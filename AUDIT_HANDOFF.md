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
| `a24730d` | Observer and validation complete and flush before scanner checkpoint persistence | 25 targeted checks passed; observer/validation/flush failures do not advance checkpoint; remote tree verified |
| `b1c8c09` | Approved conservative BE-first for already-armed historical OHLC dual touches | 27 targeted checks passed; LONG/SHORT unit and end-to-end coverage; live trader paths isolated |

Remote branch head expected before this policy-completion handoff update: `b1c8c097afb6cf4e69f1d525237fed93aae28ec2`.

## Completed safe stage: validation chronology

Commit `8181a7d` completed the prior diagnostics-first stage:

- Activation candles are now evaluated for stop/target outcomes.
- Unactivated pending setups persist progress and expire after exactly three eligible candles.
- Multi-candle refresh validation now preserves causal `update -> evaluate/register` ordering and matches one-candle processing.
- BE+TP2 same-candle behavior was locked by a diagnostic test only; its optimistic TP2-first policy was not changed.
- Strategy parameters, entry/SL/TP/RR, promotion rules, alerts, and the protected long-only baseline were unchanged.
- Validation and alerts remain OFF.

## Completed safe stage: checkpoint ordering

Commit `a24730d` completed the checkpoint failure regression stage:

- Observer evaluation runs before validation.
- Validation is explicitly flushed before checkpoint persistence.
- Observer, validation, or validation-flush failure propagates and does not advance the scanner checkpoint.
- In-memory scanner watermark/state is updated only after the checkpoint succeeds.
- No strategy parameters, entry/SL/TP/RR, promotion rules, alert rules, or protected long-only baseline behavior changed.
- Validation and alerts remain OFF.

## Completed safe stage: conservative BE-first OHLC policy

Commit `b1c8c09` implements the approved isolated policy:

- For historical OHLC only, if BE was already armed before the candle and both BE and TP2 are touched, LONG and SHORT resolve at BE.
- Exact equality at BE or TP2 counts as a touch.
- BE-only, TP2-only, and no-touch behavior is unchanged.
- Candle close does not infer intrabar order.
- Live timestamped trader paths do not use the historical OHLC resolvers; their rule remains first actual execution event wins.
- No strategy parameters, entry/SL/TP/RR, promotion rules, baseline, validation enablement, or alert behavior changed.
- Validation and alerts remain OFF.

## Confirmed critical findings still open

1. `pnf_mvp/strategy_validation.py::_should_activate`
   - Activation is based on candle close, not limit touch/trade-through.
   - Impact: this path is not equivalent to the stated execution model.

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

- The `8181a7d` stage passed 15 targeted validation/regression checks. The `a24730d` stage passed those 15 plus 10 focused scanner/checkpoint checks. The `b1c8c09` stage passed 20 validation tests plus 7 focused scanner/checkpoint checks; earlier manual persistence checks passed as listed above.
- The audit runtime lacks `pytest` and `pyarrow`; therefore no claim of a clean full-suite run has been made.

## Next smallest safe stage

Stop after the approved isolated policy stage:

1. Keep validation and alerts OFF.
2. Do not run broad historical recomputation yet; the statistical impact is limited to already-armed dual-touch rows but has not been quantified on a frozen exact-baseline dataset.
3. Treat close-based activation versus limit touch/trade-through as the next separate execution-model decision.
4. Do not request Windows installation until one consolidated package and procedure are verified.

## Recommended model routing

- Use `gpt-5.6-sol` with medium effort for code inspection, tests, implementation, and GitHub work.
- Use Astra with medium effort only for final judgment on methodology or result-changing execution rules.
- When delegating, use a narrow task with no inherited full conversation and require a concise report.

## Prompt for the next chat

> Read `AUDIT_HANDOFF.md` from branch `feature/binance-microstructure-collector` in `lykos1974/sm`. Continue only the “Next smallest safe stage”. Use diagnostics-first, do not change strategy parameters or the protected long-only baseline, keep validation and alerts OFF, and do not ask me to install anything until one consolidated package is verified. Keep updates concise and maintain GitHub.
