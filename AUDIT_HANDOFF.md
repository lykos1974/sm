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
| `ff40ae4` | Isolated trade-through activation and metric-denominator regressions | Added before runtime implementation; covers LONG/SHORT tick boundaries, activation-candle outcomes, provenance, expiry, live isolation, and AMBIGUOUS metric bounds |
| `8df34e9` | Historical trade-through outcomes and metric bounds | Explicit tick trade-through, activation-candle STOPPED/AMBIGUOUS persistence, and pessimistic/optimistic headline metrics |
| `839ab0d` | Fail-closed tick provenance configuration | Historical scanner validation/backfill require explicit per-symbol tick plus source; flags remain OFF |
| `933c691` | Complete activation regression coverage | Updated chronology, restart, diagnostic, and updater tests for the approved contract |
| `484c6da` | Consolidated updater manifest | Pins the eight approved runtime files while preserving canonical verification, backup, rollback, and settings safety |

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

## Confirmed critical finding still open

1. `live_mexc_forward_trader.py`
   - The legacy MEXC forward path treats `ORDER_SENT` as open for exit evaluation without first proving an exchange fill.
   - Binance live execution and `mexc_pole_live_trader.py` instead require exchange `FILLED`/executed-volume or open-position evidence.
   - This separate live-path finding remains open and was not changed or combined with the historical activation stage.

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

`WINDOWS_AUDIT_UPDATE.ps1` is the single Windows procedure. It now pins SHA-256 hashes for the eight approved runtime files, refuses apply unless services are declared stopped, backs up code/settings/relevant SQLite files, verifies copy/import/byte-compilation, automatically restores code after an apply failure, and supports explicit rollback. It never starts a service and never enables validation or alerts.

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

## Completed activation contract implementation

The user approved the contract with the mandatory requirement that AMBIGUOUS outcomes remain visible in headline denominators. Remote commits `ff40ae4`, `8df34e9`, `839ab0d`, `933c691`, and `484c6da` implement it test-first:

- LONG activates only at `low <= ideal_entry - tick`; SHORT only at `high >= ideal_entry + tick`. Equality and sub-tick penetration do not fill.
- Every tick must be explicit per symbol, positive, and paired with a non-empty provenance source. The exact tick and source are persisted on activation. There is no universal default.
- Gap-through candles fill at `ideal_entry`; no favorable price improvement is credited.
- Any stop touch on the activation candle resolves as `STOPPED`, including candles that also touch a target.
- A target touch without a stop on the activation candle is persisted as `AMBIGUOUS`, with `resolved_price=NULL`, pessimistic `-1R`, and optimistic TP1/TP2 R derived from the unchanged entry/invalidation/target prices.
- AMBIGUOUS has no arbitrary `0R`. Its count remains in the headline denominator, and the export reports pessimistic/optimistic win-rate and realized-R bounds. Missing legacy bounds cause R-bound metrics to report unavailable rather than silently substitute zero.
- Exact three-eligible-candle expiry is unchanged.
- Timestamped live traders remain isolated and retain first actual exchange fill/event semantics.
- Additive SQLite migrations only; no database reset or deletion.

The configured `symbol_ticks` map is intentionally empty while validation is OFF. Historical validation and backfill now fail closed unless authoritative per-symbol tick values and provenance sources are supplied; no tick was guessed.

Verification: `60` direct `unittest` checks plus `20` dependency-free checkpoint/scanner checks passed (`80` targeted checks), followed by repository byte-compilation and `git diff --check`. No broad recomputation was run. Validation and alerts remain OFF.

Expected result-changing scope once explicitly configured and enabled: historical validation/backfill activation membership, activation timestamps, activation-candle STOPPED/AMBIGUOUS outcomes, and their reported headline metric bounds. Signal generation, setup promotion, entry/SL/TP/RR values, post-activation management, operational alerts, live exchange execution, and the protected long-only baseline logic are unchanged.

## Test limitations

- The latest stage passed `80` targeted checks, plus byte-compilation and diff validation.
- The audit runtime still lacks `pytest`, `pyarrow`, and PowerShell. Pytest-style targeted tests were invoked through a dependency-free harness; the revised portability logic received static and behavioral cross-platform tests but has not yet had a native-Windows rerun. No clean full-suite claim is made.

## Next smallest safe stage

Stop after the isolated activation-policy stage:

1. Keep validation and alerts OFF.
2. Do not run broad historical recomputation.
3. Independently obtain and verify authoritative exchange tick sizes for the intended historical symbols, then record each value and source in the explicit provenance map. Do not enable validation or backfill during that metadata-only stage.
4. After tick provenance is reviewed, run one small frozen-fixture validation before any broader historical work.
5. Treat the legacy MEXC `ORDER_SENT` lifecycle as a separate fail-closed audit stage; do not combine it with activation-policy work.

## Recommended model routing

- Use `gpt-5.6-sol` with medium effort for code inspection, tests, implementation, and GitHub work.
- Use Astra with medium effort only for final judgment on methodology or result-changing execution rules.
- When delegating, use a narrow task with no inherited full conversation and require a concise report.

## Prompt for the next chat

> Read the latest `AUDIT_HANDOFF.md` from branch `feature/binance-microstructure-collector` in `lykos1974/sm`. Continue only the “Next smallest safe stage”. Diagnostics-first, independently source explicit tick size and provenance per intended symbol, but keep validation and alerts OFF and do not run backfill or broad recomputation. Preserve all strategy parameters and the protected long-only baseline. Keep updates concise and maintain GitHub.
