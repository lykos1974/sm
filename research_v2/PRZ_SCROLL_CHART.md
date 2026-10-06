# Scrollable PRZ chart (offline research)

The standalone `prz_scroll_chart.py` renders the complete 2024 BTCUSDT P&F
chart (100-point boxes, three-box reversal) and all PRZ projections recorded
in the pinned exploratory annual report. The output is one self-contained
HTML file. It can be opened locally without a server, Python service, network,
operational database, order system, or installation.

From the `feature/du-plessis-poles-v1` checkout on Windows:

```powershell
python -B -m research_v2.prz_scroll_chart `
  --report 'H:\pnf screener\research_snapshots\PRZ_x_break_BTC2024_20261006.json' `
  --candles 'H:\pnf screener\research_snapshots\BINANCE_UM_BTC_2024_frozen_20261001_040822_8874345\results\candles_1m.csv' `
  --output 'H:\pnf screener\research_snapshots\BTC_2024_all_PRZ_scroll.html'
Start-Process 'H:\pnf screener\research_snapshots\BTC_2024_all_PRZ_scroll.html'
```

The input candle SHA-256 must equal
`8045aa135a611d4b4fc2ca0cde9a8fa68905ad4480054399f33ed77ab8f6851f`;
the complete year and report schema/parameters are checked. An existing HTML
file is never overwritten. The output embeds all P&F columns and projected
zones; it is a static research artifact, with horizontal scrolling, month jump,
PRZ selector, previous/next, zoom and LONG/SHORT visibility. X/A/B/C pivot
connections and shaded D zones follow the report's already frozen decision
facts. Contact, far-edge test and invalidation/expiry events are marked.
The vertical scale adapts to the visible columns. The report SHA-256 appears
in the page for traceability.

The 28 zones, 14 contacts, 13 full tests, 8 invalidations and 20 expiries
are **operator-reported event counts** for this pinned run. They are not
independent trades and are not evidence of profitable execution. A standard
Gartley is invalid after an X crossing; the separate three pole-context
matches describe historical context only. All live and validation features
remain OFF.
