# PnF Forensic Audit Handoff

Updated: 2026-09-22
Repository: `lykos1974/sm`  
Branch: `feature/binance-microstructure-collector`  
Pull request: `#350`

## Operating state

- Crypto collector and persisted-state scanner may continue running.
- Consolidated Windows update installed successfully on 2026-09-22; backup: `H:\pnf screener\sm_repo\_audit_update_backups\20260922_081343`.
- Post-install persisted-state scanner bootstrap passed for all 12 configured symbols with `lag_candles=0`.
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
| `2f5cc97` | Final policy-stage handoff | Approved behavior and remaining limitation recorded; no runtime code change |
| `184e7a9` | Windows updater portability fixes | Late `SourceRoot` resolution plus strict UTF-8 CRLF/LF canonical verification; backups/rollback/settings guards preserved |
| `45e76b3` | Diagnostic activation and updater portability regression suites | 13 activation diagnostics plus 5 updater portability tests passed; no strategy runtime change |

Clean-checkout verification base: `2f5cc971c93028725d7624dcabc2a5cdd59cb363`.

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
   - LONG activates only when `close <= ideal_entry`; SHORT only when `close >= ideal_entry`.
   - Equality of the **close** activates. Equality/touch by low or high alone does not.
   - `open` is neither loaded nor accepted by `update_pending_with_candle`, so gap-open policy cannot be represented.
   - On activation, `activated_price` is always `ideal_entry`, not the close or open.
   - The complete activation candle H/L range is immediately reused for stop/target resolution even though extrema may have occurred before close-based activation.
   - Touch-only candles count toward the exact three-candle expiry and can expire a setup that an OHLC limit-touch model would have filled.
   - Impact: this path is not equivalent to either H/L limit touch or timestamped trade-through.
2. `live_mexc_forward_trader.py`
   - The legacy MEXC forward path treats `ORDER_SENT` as open for exit evaluation without first proving an exchange fill.
   - Binance live execution and `mexc_pole_live_trader.py` instead require exchange `FILLED`/executed-volume or open-position evidence.
   - This separate live-path finding was diagnosed only and was not changed in this stage.

## High-risk reproducibility gaps

- Claimed modules `strategy_setup_generation.py` and `strategy_setup_generation_incremental.py` are absent from this branch.
- Full XAUUSD figures (`142` opportunities, `112` resolved, `+169.5645R`, DD `2R`) have not been independently reproduced from a clean checkout and frozen manifest.
- Existing historical/live shadow comparisons can share the same underlying PnF/strategy bug and are not independent proof of causality.

## Confirmed causal components

- Completed PnF columns were not observed to mutate after reversal.
- PnF signal checks use the current close update; no future column is referenced.
- `structure_engine.py` derives completed swing levels from `columns[:-1]`; no retrospective pivot timestamp reassignment was found in the inspected path.
- These findings do not validate the execution engine or reported trading metrics.

## Completed safe stage: consolidated clean-checkout verification

Starting from a new checkout at exact commit `2f5cc97`:

- All required runtime files were present and eight scanner/strategy modules imported successfully.
- Python byte-compilation completed for the scanner, historical validator, live trader files, and targeted tests.
- Fresh scanner, validation, and observer SQLite schemas passed `PRAGMA integrity_check`; required additive columns and tables were present.
- Atomic checkpoint round-trip, stale-watermark refusal, non-contiguous-column rejection, and stateful engine resume passed.
- Observer lifecycle/migration and downstream ordering passed; observer, validation, and validation-flush failures still cannot advance the checkpoint.
- Historical closed-candle handling, causal PnF snapshots, validation chronology, exact expiry, and conservative LONG/SHORT BE-first regressions passed.
- The historical OHLC resolver remains absent from all three timestamped live trader paths.
- `46` targeted regression checks passed (`29` direct `unittest` checks plus `17` dependency-free checkpoint/scanner checks).
- `strategy_validation_enabled=false` and `operational_alerts_enabled=false` were verified before and after testing.

No strategy runtime file, strategy parameter, entry/SL/TP/RR rule, promotion rule, protected baseline, or enablement flag was changed during this verification.

`WINDOWS_AUDIT_UPDATE.ps1` is the single Windows procedure. It pins SHA-256 hashes for the six approved runtime files, refuses apply unless services are declared stopped, backs up code/settings/relevant SQLite files, verifies copy/import/byte-compilation, automatically restores code after an apply failure, and supports explicit rollback. It never starts a service and never enables validation or alerts.

## Completed safe stage: Windows installation and bootstrap

- The consolidated update completed with `UPDATED AND VERIFIED` on Windows.
- Backup and rollback material was created at `H:\pnf screener\sm_repo\_audit_update_backups\20260922_081343`.
- The scanner then bootstrapped all 12 configured symbols from persisted state with `lag_candles=0` for every symbol.
- Strategy validation and operational alerts remained OFF.
- No baseline reset, database deletion, broad recomputation, or strategy-parameter change was performed.
- Two procedure portability defects were observed before the successful apply: default `SourceRoot` could fail through `$PSScriptRoot` binding, and raw SHA-256 checks treated CRLF/LF-only Python content as different. These are procedure defects only; the installed canonical Python content and scanner bootstrap passed.

## Completed safe stage: activation diagnostics and updater portability

Activation audit:

- Scanner validation and `strategy_historical_backfill.py` share the same close-only `StrategyValidationStore` path.
- Independent `research_v2` labeling uses inclusive H/L touch (`low <= entry` / `high >= entry`).
- The timestamped microstructure evaluator distinguishes touch from proof of fill and requires aggressor-confirmed trade-through by one tick.
- Binance live activation follows the exchange's actual `FILLED` status; the guarded MEXC pole path checks filled volume/order state/open position. Neither uses `_should_activate`.
- LONG and SHORT diagnostics cover close equality, H/L equality, touch-only reversal, gap/open blindness, fill price, activation-candle outcome, exact three-candle expiry, continuous batch processing, and per-candle database restart equivalence.
- Current continuous and restart processing are equivalent under the existing close-only model.

Updater portability:

- Default `SourceRoot` is now resolved after parameter binding from `$PSScriptRoot`, with `$MyInvocation.MyCommand.Path` as a fail-closed fallback.
- Runtime Python files use strict UTF-8 canonical hashes that normalize CRLF to LF only. BOM, bare-CR, encoding, or content changes still fail verification.
- Source, temporary copy, and installed target all receive the same canonical-content verification.
- Raw database/backup hashes, settings OFF guards, backups, automatic code restore, and explicit rollback remain unchanged.

Verification: `47` direct `unittest` checks plus `17` dependency-free checkpoint/scanner checks passed (`64` total), followed by repository byte-compilation and `git diff --check`. `pnf_mvp/strategy_validation.py` and every strategy/runtime parameter remained unchanged.

### Exact execution-policy decision awaiting approval

Recommended conservative historical-OHLC contract, not yet implemented:

1. A pending LONG fills only after trade-through by at least one known tick (`low <= ideal_entry - tick`); SHORT symmetrically requires `high >= ideal_entry + tick`. Equality-only is **not** fill proof.
2. A gap through the limit fills at `ideal_entry` (no favorable open-price improvement in reported results).
3. On the activation candle, any stop touch resolves conservatively as STOPPED; target touch without a stop is AMBIGUOUS/excluded because OHLC cannot prove it occurred after the fill; otherwise the trade becomes active.
4. Unfilled orders still expire after exactly three eligible closed candles.
5. Timestamped live data remains authoritative: the first actual exchange fill/execution event wins.

Approval of this complete contract—or an explicit choice of inclusive H/L touch instead of one-tick trade-through—is required before changing `_should_activate`, candle inputs, or outcome handling.

## Test limitations

- The latest stage passed `64` targeted checks, plus byte-compilation and diff validation.
- The audit runtime still lacks `pytest`, `pyarrow`, and PowerShell. Pytest-style targeted tests were invoked through a dependency-free harness; the revised portability logic received static and behavioral cross-platform tests but has not yet had a native-Windows rerun. No clean full-suite claim is made.

## Next smallest safe stage

Stop after the diagnostic activation/portability stage:

1. Keep validation and alerts OFF.
2. Do not run broad historical recomputation.
3. Do not change activation behavior until the complete historical-OHLC execution contract above is explicitly approved.
4. Treat the legacy MEXC `ORDER_SENT` lifecycle as a separate fail-closed audit stage; do not combine it with activation-policy work.

## Recommended model routing

- Use `gpt-5.6-sol` with medium effort for code inspection, tests, implementation, and GitHub work.
- Use Astra with medium effort only for final judgment on methodology or result-changing execution rules.
- When delegating, use a narrow task with no inherited full conversation and require a concise report.

## Prompt for the next chat

> Read the latest `AUDIT_HANDOFF.md` from branch `feature/binance-microstructure-collector` in `lykos1974/sm`. Continue only the “Next smallest safe stage”. Keep validation and alerts OFF and preserve all strategy parameters and the protected long-only baseline. Do not change `_should_activate` until the historical-OHLC execution contract is explicitly approved. Keep updates concise and maintain GitHub.
