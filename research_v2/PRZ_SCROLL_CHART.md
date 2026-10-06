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
  --output 'H:\pnf screener\research_snapshots\BTC_2024_all_zones_v2.html'
Start-Process 'H:\pnf screener\research_snapshots\BTC_2024_all_zones_v2.html'
```

The input candle SHA-256 must equal
`8045aa135a611d4b4fc2ca0cde9a8fa68905ad4480054399f33ed77ab8f6851f`;
the complete year and report schema/parameters are checked. An existing HTML
file is never overwritten. The output embeds all P&F columns and projected
zones; it is a static research artifact, with horizontal scrolling, month jump,
PRZ selector, structural-zone selector, independent nonconsecutive Fibonacci
selector, previous/next, zoom and LONG/SHORT visibility. X/A/B/C pivot
connections and shaded D zones follow the report's already frozen decision
facts. Contact, far-edge test and invalidation/expiry events are marked.
The vertical scale adapts to the visible columns. The report SHA-256 appears
in the page for traceability.

## Separate multi-column structural S/R layer

The initial fine-grid structural proposal was **rejected for visualization**:
the Windows operator reported 5,039 overlapping labels and an unreadable
chart. That count came from nearly every 100-point pivot; it must not be
called a set of significant levels. The historical fine-grid prototype
remains in code for reproducibility but is never used by the chart.

The revised `pnf-multicolumn-sr-v2-coarse` profile replays the **same frozen
closes** in a separate 1,000-point/3-reversal P&F chart. It reads only
confirmed pivots. A zone is emitted **on confirmation of the second like-kind
pivot**, after an intervening opposite pivot. A HIGH pair gives RESISTANCE;
a LOW pair gives SUPPORT. The two levels must be within one coarse box,
the intervening excursion at least three coarse boxes, and the two pivots
at most 24 coarse columns apart. The most recent qualifying prior pivot is
selected per confirmation. All settings are explicit exploratory research
conventions, not Du Plessis or Carney rules or fitted to trade outcomes.
Replaying the complete frozen input fixes the coarse column sequence and
timestamps; no future pivot can change an already emitted zone. The original
fine-grid Gartley pivots are checked against the replayed 100-point chart.

This structural layer is colored amber/violet and is initially **not drawn**;
select a zone to show only that zone at a time. The two pivot marks and level
extension (20 fine columns) are display aids, not assertions of live validity.
The original 28 Gartley PRZ stay visible and retain their exact meaning.
No Fibonacci projection is required in the coarse S/R layer, so these zones
are **not called harmonic PRZ**. A separate causal study can test Fibonacci
confluence without silently changing either layer.

## Nonconsecutive coarse harmonic projections (separate research convention)

`pnf-multicolumn-gartley-v1` uses the same pinned close-confirmed 1000/3
coarse P&F replay. At confirmation of C it enumerates alternating X/A/B/C
pivots within a **24-column lookback**, requiring at least one skipped column.
The first coarse column may be X or O; the chronology check uses its actual
kind and verifies every following reversal instead of imposing an O start.
Intervening confirmed extremes must stay inside each selected leg's endpoint
range; otherwise selecting distant endpoints would conceal a more extreme
pivot. The chosen four pivots are mapped to logical consecutive indices solely
to reuse the existing restricted Gartley ratio and D-projection routine:
B/XA 0.588–0.648, C/AB 0.382–0.886, reciprocal BC 1.13–1.618,
0.786 XA versus AB=CD, and projected zone width at most one coarse box.
This is an explicitly versioned **implementation assumption**, not a claim
that a textbook requires a 24-column horizon or this intermediate-extreme
rule. The C confirmation close must still be on the approach side of D;
later zone contacts and invalidation are **not** tracked in this layer.
Projection time is C confirmation, never the earlier pivot extreme.

These candidate projections appear in their own selector, initially with
none selected. A selected candidate draws one cyan X/A/B/C path and a short
shaded D zone. It neither changes the existing 28 consecutive Gartley PRZ
nor the coarse structural S/R labels. A particular visible formation may
still be excluded by geometry or ratios; annual count must be measured from
the pinned operator input, not inferred from screenshots. No order, fill,
trade or profitability is implied.

## Operator-reported full-year chart (2026-10-06)

From commit `6f01f4f8f2c13ac5e31669990d6f41e9b8651219`, the Windows operator
reported successful creation of
`H:\pnf screener\research_snapshots\BTC_2024_zones_20261006_120726.html`.
The console output reported 6,519 fine P&F columns, the unchanged 28
Gartley PRZ, and **58** coarse structural zones, with execution OFF. The
frozen candle SHA-256 was
`8045aa135a611d4b4fc2ca0cde9a8fa68905ad4480054399f33ed77ab8f6851f`;
the input annual report SHA-256 was
`d29f3924bb224d4ffca857de564478f61f6cc48ded0727b61e9e89e6f20c6e57`.
This is operator-provided console evidence, not an independent inspection
of the local HTML or 2024 candle file. The 58 zones are exploratory S/R
labels, not confirmed harmonic PRZ, trades, or profitable opportunities.
The marked late-February/early-March example has not yet been matched to
an exact zone identity and decision time.

The 28 zones, 14 contacts, 13 full tests, 8 invalidations and 20 expiries
are **operator-reported event counts** for this pinned run. They are not
independent trades and are not evidence of profitable execution. A standard
Gartley is invalid after an X crossing; the separate three pole-context
matches describe historical context only. All live and validation features
remain OFF.
