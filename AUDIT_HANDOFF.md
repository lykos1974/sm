# PnF Forensic Audit Handoff

Updated: 2026-09-24
Repository: `lykos1974/sm`
Branch: `feature/binance-microstructure-collector`
Pull request: `#350`

## Operating state

- Strategy validation remains **OFF**.
- Operational alerts remain **OFF**.
- Signal generation, strategy classification, entry/SL/TP/RR values, promotion rules, PnF, structure, live traders, and the protected long-only baseline were not changed.
- No broad recomputation, database reset, installation, or service start was performed.
- A new hash-pinned reversible Windows updater is prepared for this final repair, but it must not be executed until an independent audit returns the required PASS verdict.

## Final release-gate blocker repair

The three independently reproduced blockers after `9c4337c8` are repaired in one narrow release series:

- `WINDOWS_AUDIT_UPDATE.ps1` pins the final runtime bytes, canonicalizes CRLF/LF safely, backs up every replaced file into a timestamped manifest, restores the exact prior files, and treats the user's `settings.json` as preflight-only state that is never packaged or overwritten.
- Tick provenance now requires a separate explicit `symbol_identities` allow-list. Provider, venue, instrument type, native symbol, and source symbol must all exactly match it before the database is opened. Unknown symbols and incompatible identities fail closed.
- Exporter and evaluator use the same economic chronology, `created_ts` then `setup_id`, before drawdown and losing-streak calculations. Totals, denominator, expectancy, win rate, drawdown, and losing streak reconcile under shuffled insertion and timestamp ties.

Validation and alerts remain OFF. The production `symbol_identities` and `symbol_ticks` maps remain empty; no exchange tick size was invented.

## Consolidated VALIDATION ENGINE REPAIR release

Test-first commit series:

1. `6d402931787d344c04e0b2640b2d738ad7d11bb0` — consolidated regression tests.
2. `7535728d7932a438ebc5e89ff1f9b9154e66da92` — expanded crash, replay, provenance, migration, chronology, accounting, and clean-equivalence acceptance coverage.
3. `8b53d41364741e2f7bfbb6fb43dbfeff239d1daf` — validation engine atomicity, branch chronology, and reconciled accounting implementation.

Runtime files changed:

- `pnf_mvp/strategy_validation.py`
- `pnf_mvp/strategy_trade_export.py`
- `pnf_mvp/strategy_evaluator.py`

## Previous behavior removed

- A candle could commit activation and its watermark before the activation-candle outcome, so replay after a crash could permanently lose STOPPED or AMBIGUOUS state.
- Multiple setups evaluated for one candle could be partially committed.
- Legacy nonterminal rows with no safe watermark/provenance could continue processing.
- Tick identity relied on incomplete/free-form provenance and could be interpreted after registration.
- Activation-candle favorable extremes could arm TP1/BE even though OHLC cannot establish that the favorable movement occurred after fill.
- AMBIGUOUS metrics used incomplete/proxy accounting paths, some exporters omitted unresolved categories, and TP2 accounting could substitute a fixed RR assumption.

## Current implementation

### Atomicity and replay

- Each complete `update_pending_with_candle` call uses one SQLite savepoint across every eligible setup for that symbol/candle.
- Activation, activation-candle outcome, TP1/BE state, expiry, resolution, branch state, excursion state, and `last_evaluated_candle_ts` commit together or roll back together.
- `_mark_dirty` and `commit_every` cannot commit while the candle transaction is active.
- Crash injection covers every DML, every SQL operation inside the savepoint, and the `after_savepoint`, `before_release`, and `after_release` boundaries. Replay produces the same canonical bytes as uninterrupted processing.
- Repeated or older timestamps perform zero DML and zero lifecycle transitions. This is covered for LONG/SHORT, pending, activation, TP1/BE-armed, branched, resolved, restart, scanner-checkpoint retry, and per-candle/batch processing.
- Migrated nonterminal rows with a NULL watermark or incomplete legacy provenance fail closed and remain unchanged; no watermark is inferred.

### Frozen structured tick provenance

- Tick provenance is validated and frozen at setup registration.
- Required fields are `provider`, `venue`, `instrument_type`, `native_symbol`, `source_symbol`, positive finite `tick_size`, `provenance_timestamp`, and `provenance_version`.
- `native_symbol` and `source_symbol` must exactly match the registered setup identity. Missing, NaN, infinity, zero, negative, unknown-symbol, and mismatched values fail before setup DML.
- Free-form `source` text remains descriptive only and cannot establish symbol identity.
- Frozen provenance survives PENDING, ACTIVE, EXPIRED, AMBIGUOUS/branched, resolution, restart, and later settings changes.

### Migration and disabled path

- Required legacy columns are added before indexes that reference them.
- Empty, immediately previous, and pre-`activation_status` databases preserve rows, survive repeated startup, and pass `PRAGMA integrity_check`.
- Scanner startup returns immediately when validation is OFF, before parsing or dereferencing `strategy_validation_execution`; missing, null, and malformed validation configuration is covered.

### Activation-candle chronology

- Historical LONG activation requires `low <= ideal_entry - one frozen symbol tick`; SHORT requires `high >= ideal_entry + one frozen symbol tick`. Equality-only is not a fill.
- Gap-through fills at the unchanged `ideal_entry`; no favorable improvement is credited.
- Pre-fill activation-candle movement cannot arm TP1/BE.
- Any activation-candle stop touch resolves STOPPED.
- Target-before/after-fill uncertainty persists two explicit branches: a completed terminal-target branch and a still-active branch. The active branch continues across later candles and survives restart/replay.
- When all branches complete, lower/upper R comes from the completed branch outcomes. Partial-position TP1/BE or TP2 R is applied exactly once using the setup's actual prices.
- AMBIGUOUS/branched setups remain persisted and counted; no arbitrary `0R` is assigned.
- The approved historical BE-first rule for an already-armed BE plus TP2 same-candle touch is unchanged. Timestamped live execution remains isolated and first actual fill/event wins.

### Metrics and accounting

- The exporter now loads every registered setup, including pending and expired/missed rows.
- One accounting function supplies headline and grouped counts and declares the same outcome denominator everywhere.
- Resolved, ambiguous/branched, pending, expired-after-activation, and missed-never-activated rows are reported separately and reconcile to registered rows.
- Pessimistic/optimistic total R, expectancy, win rate, and drawdown use actual per-trade entry, stop, TP1, TP2, and resolved prices.
- No hard-coded TP2 RR proxy remains. Partial-BE and partial-TP2 accounting is included exactly once.
- Incomplete open-branch bounds remain unavailable instead of silently shrinking the denominator or substituting zero.

## Verification

- Full repository suite: `721 passed, 136 subtests passed`.
- Targeted crash/replay/provenance/chronology/accounting suite is included in that total.
- Continuous, per-candle restart, retry, and replay canonical results are byte-identical for LONG and SHORT.
- Multiple setups in one candle roll back as a unit at every injected intermediate SQL point.
- Repeated migrations preserve rows and pass SQLite integrity checks.
- Repository byte-compilation and `git diff --check` pass.
- Final remote bytes and the full suite were rechecked from a clean checkout after push; an independent re-audit remains required before proceeding.

## Remaining blockers

1. A clean independent re-audit of the final remote HEAD must return `PASS FOR REVERSIBLE WINDOWS INSTALLATION WITH VALIDATION/ALERTS OFF` before Windows installation.
2. Existing legacy nonterminal rows without a safe watermark and complete structured tick provenance intentionally fail closed. Their disposition requires a separate reviewed migration decision; values must never be inferred.
3. `pnf_mvp/settings.json` intentionally keeps both validation identity and tick maps empty while validation is OFF. Authoritative per-symbol identities and structured tick provenance are required before any future enablement.
4. The legacy `live_mexc_forward_trader.py` `ORDER_SENT`-without-proven-fill issue remains open and was not combined with this release.
5. Full XAUUSD headline figures and missing historical generation modules remain unreproduced audit gaps; no claims were made from them here.

## Next gate

Independently re-audit the standalone MEXC order-discovery CLI against the official history-orders endpoint. Verify the fixed GET-only endpoint and that the exact sorted and URL-encoded business parameter string transmitted is included after the API key and request time in the HMAC payload; authentication headers must not enter the signed parameters. Confirm duplicate-key rejection, exact Decimal quantities and prices, redaction, and offline redirect denial. The history response supplies updateTime but no proven fill timestamp; fill_time stays null, and the existing shadow check must verify deal timestamps independently. Adapter, fill reconciliation, trader, validation and alerts remain OFF.
