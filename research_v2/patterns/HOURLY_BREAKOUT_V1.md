# Hourly breakout v1 — preregistered research hypothesis

This separate, default-OFF research simulation tests one trend continuation
hypothesis. It does not change the protected pullback baseline, Du Plessis
poles, trader, validation, alerts, or any operational database.

## Fixed decision and execution policy

- Source: pinned Binance USD-M BTCUSDT 1m OHLC for the full UTC year.
- Aggregate each aligned group of 60 completed one-minute candles into one
  completed UTC H1 candle. Reject gaps or partial hours.
- LONG signal only if the completed H1 close is **strictly above** the highest
  high of the preceding 20 completed H1 candles. At least 21 prior hours are
  required so 14 true-range observations can each use a previous close.
- ATR14 = arithmetic mean of 14 H1 true ranges ending at the signal hour.
  Stop distance = 2 ATR14. A zero or invalid distance creates no signal.
- Enter at the **next 1m open**, never at the retrospective breakout close.
  Stop = entry minus distance; target = entry plus 2.5 times distance.
- One position maximum. Earliest minute whose close reaches 48 hours after
  entry exits at close if neither barrier was hit. A gap through stop fills
  at the adverse open. Both stop and target in one minute: stop first.
  No synthetic trade is closed at year end: open/pending flags are reported.
- Exact Decimal arithmetic for input prices and R. Hypothetical symmetric
  round-trip costs of 0, 2, 5 and 10 bps **per side**, using entry notional
  divided by planned risk; no funding, spread dynamics, exchange fills or
  price tick rounding. Gross and modeled totals are distinct.
- One full-year run per input; no parameter sweep. Strategy ID
  `btc-hourly-breakout-v1` is not available to operational components.

These lookback, ATR multiplier, target, timeout and cost scenarios are our
predeclared **research conventions**, not sourced from Du Plessis or a
profitability claim. Both 2024 and 2025 have previously been viewed for other
strategies. Comparing them is exploratory, not a clean untouched holdout.
A new prospective cohort and independently audited execution costs are the
required gates before any strategy claim.

## One local command

The code checkout is
`H:\pnf screener\research_snapshots\PRZ_pole_code_20261006`.
Fetch the branch and switch the detached checkout to the commit of this
document, then paste the block below. Results go only to two new research
JSON files in a timestamped folder.

```powershell
$code = 'H:\pnf screener\research_snapshots\PRZ_pole_code_20261006'
$root = 'H:\pnf screener\research_snapshots'
$dir = Join-Path $root ('BTC_hourly_breakout_' + (Get-Date).ToUniversalTime().ToString('yyyyMMdd_HHmmss_fffffff'))
New-Item -ItemType Directory -Path $dir -ErrorAction Stop | Out-Null
Push-Location $code
try {
    python -B -m unittest -q tests.test_hourly_breakout_v1
    if ($LASTEXITCODE -ne 0) { throw 'Offline tests failed' }
    python -B -m research_v2.patterns.hourly_breakout_v1 `
      --candles (Join-Path $root 'BINANCE_UM_BTC_2024_frozen_20261001_040822_8874345\results\candles_1m.csv') `
      --sha256 8045aa135a611d4b4fc2ca0cde9a8fa68905ad4480054399f33ed77ab8f6851f `
      --expected-minutes 527040 --output (Join-Path $dir 'BTC_2024.json')
    if ($LASTEXITCODE -ne 0) { throw '2024 failed; stop' }
    python -B -m research_v2.patterns.hourly_breakout_v1 `
      --candles (Join-Path $root 'BINANCE_UM_BTC_2025_frozen_20261007_221948_5274615\candles_1m.csv') `
      --sha256 4e33b5c2a549b1eac26adfec022aab4cadce92261c13526a268f079225d16cab `
      --expected-minutes 525600 --output (Join-Path $dir 'BTC_2025.json')
    if ($LASTEXITCODE -ne 0) { throw '2025 failed; stop' }
    Write-Host "RESULT_DIR: $dir"
} finally { Pop-Location }
```

Review each JSON's signals, resolved trades, pending/open flags, gross R,
quarterly breakdown, drawdown, cost scenarios, and source hash. Preserve
the trade ledger for chronology audit. The annual summaries omit the long
trade ledger from the console but keep it in the JSON.

## Verification

Six synthetic offline tests cover a close-confirmed breakout, next-minute
entry, same-minute double-barrier stop priority, no future signal, prefix
invariance, deterministic restart, incomplete-hour rejection, source SHA
and exclusive report. They do not prove live fill quality. Compilation passes.
