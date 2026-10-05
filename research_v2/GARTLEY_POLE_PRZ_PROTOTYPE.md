# Restricted Gartley PRZ × Du Plessis pole, research prototype v1

Implementation is isolated in `gartley_pole_prz.py`; the optional bounded CLI is
`gartley_pole_prz_smoke.py`. It annotates pole decisions, without filtering the
baseline simulation or authorizing execution. There is no operational DB,
network, order, UI, strategy-engine, or trader integration.

The cited Carney pages below came from the supplied research brief. The book
pages were not independently available or verified in this checkout. This
prototype is a restricted mechanical interpretation, not a complete Gartley
implementation or a profitability claim.

| Source claim in brief | Mechanical convention implemented | Exclusion/test |
|---|---|---|
| pp. 47–48: CD violates B | Bullish closed close `< B`; bearish `> B`; equality fails | `test_b_equality_does_not_count_as_violation` |
| pp. 91–92: standard B at 0.618 ±0.03 | Exact Decimal inclusive `[0.588,0.648]` | `test_ratio_boundaries_and_width` |
| p. 92: C `[0.382,0.886]`, BC `[1.13,1.618]`, XA 0.786, AB=CD | Consecutive confirmed column endpoints; `kBC=1/rC`, equivalent AB=CD, Decimal projection | `test_equivalent_abcd_and_mirror`; no independent BC confluence vote |
| p. 92: entire PRZ tested | `L=min(D_XA,D_ABCD)`, `U=max(...)`, width ≤ one 100 USDT box; closed close reaches far edge after creation | `test_partial_then_full_then_pole` |
| pp. 153–154: terminal bar tests last level | Closed 1m **close** is a research convention, not an OHLC terminal-bar reconstruction | Intrabar touch and order sequence unproven |
| pp. 95, 158/161: variants and symmetry | Bullish/ bearish mirror; Deep and alternate AB=CD excluded | `test_variant_projection_and_unconfirmed` |

`D_BC=C+(B-C)/rC` equals `D_ABCD=C+(B-A)` algebraically. It is not an
independent confirmation. The one-box width, X-crossing invalidation, and
close-only test are preregistered implementation choices, not attributed to
Carney. There is no RSI/HSI, snapping, added epsilon, or result tuning.

## Event order

At each strictly increasing completed 1m close, the existing PnF engine first
updates its active column and emits any Du Plessis pole signal. The research
ledger confirms the **previous** column endpoint only after the new adjacent
column exists. Its `extreme_at` is the final update of the closed column;
`confirmed_at` and event sequence mark when that endpoint became usable.
The four most recent consecutive confirmed pivots become X/A/B/C. The
immutable zone is created when C is confirmed. At creation, prior recorded
closes since C's last extreme and the current close must still be on the approach side;
otherwise the candidate is unavailable. Future closed closes determine
first contact, strict B violation and full-zone test. A same-close full test
and pole signal is excluded. Only the immediately connected D-direction PnF
column can provide the pole; on its closing event the pole is evaluated
before expiry. X crossing invalidates before signal. At most one match is
accepted per candidate. Signal-close and known pole-extreme distances from
the frozen zone are recorded without a distance filter. Events and decisions
are append-only; exact duplicate updates do nothing, and conflicting updates
or modified confirmed pivots raise errors. No future extrema enter a decision.

## Provenance and remaining data gap

The repository has a pinned annual candle hash and an **operator-reported**
574-trade summary, but no independently received full annual event/trade
ledger or full dataset manifest. Summary counts cannot reproduce the 574
trades. The smoke CLI produces actual event records only for its bounded
input, with source hash and config in the report. It has no annual performance
claim and does not silently turn a subset of baseline trades into a filtered
execution backtest. A later annual annotation requires the exact frozen input,
all baseline signals including skipped/blocked, and a separate immutable
baseline trade ledger. Removing trades would change position availability;
that filtered replay is a separate experiment.

Bounded offline smoke, from the repository root, using a **research copy** of
the frozen input and its independently verified SHA-256:

```powershell
python -B -m research_v2.gartley_pole_prz_smoke --candles 'H:\research\candles_1m.csv' --sha256 '<actual 64-character SHA-256>' --max-candles 10000 --output 'H:\research\new_prz_smoke_report.json'
```

The output must not already exist. No market files are included here. A 10k
prefix should use seconds to minutes and memory proportional to event count;
the precise runtime depends on the local disk and PnF engine. The 2024 data
were already observed and can only be exploratory. Before empirical claims,
freeze a distinct out-of-sample interval, costs, metrics and pass criterion.

**Implementation:** the mechanics pass synthetic chronology checks, but
same-leg match feasibility fails for the combined rules, as shown below.
**Empirical evaluation:** BLOCKED. The annual ledger and an out-of-sample
protocol are also missing. This document makes no performance PASS claim.

## Structural feasibility audit (2026-10-06)

The Windows operator's 10,000-candle smoke at published commit `dc107360`
returned 4 poles, all `NO_CAUSAL_ZONE`; 53 pivot groups failed geometry and
9 failed the B ratio. Those counts alone are not evidence of rarity or edge.

An audit found two separate problems:

1. The original late-zone check scanned prior closes back to X. A standard
   bearish X high (bullish X low) naturally lies beyond the later projected
   zone, so it was incorrectly classified as a pre-creation touch. The check
   now begins at C's last extreme. A full zone test is still required strictly
   after C confirmation.
2. More fundamentally, the **specified rules cannot produce a PRZ_MATCH**
   on the connected pole leg under this PnF grid. For a bearish Gartley, X is
   the initial high H; the next O column A starts one box below H. The pole
   breakout X column D must exceed the prior three-column high by at least
   three boxes. Those columns include A, so D must reach at least H+2 boxes.
   The observed closed price necessarily crosses H before the ≥50% pole
   retracement signal, which triggers the mandatory `X_CROSSED_BEFORE_SIGNAL`
   invalidation. The bullish case mirrors this with D ≤ X−2 boxes. An exact
   bearish/bullish PnF-engine test demonstrates a valid projected zone,
   B violation, full-zone test, X invalidation and then a pole signal with
   `PRZ_AVAILABLE_NONMATCH`.

This is a **design FAIL for same-leg PRZ_MATCH feasibility**, not a negative
performance result. Do not run the annual comparison or optimize parameters
under this definition. The X invalidation is a declared prototype convention;
removing it or changing the structural relationship would be a new versioned
research hypothesis requiring an explicit source and chronology review.
