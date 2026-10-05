# PRZ tested → X broken → pole: separate research context v2

The v1 restricted Gartley and Du Plessis pole profile are unchanged. A valid
standard Gartley is **invalidated** when price crosses X. Since the required
three-box pole breakout on the connected leg also crosses X, a same-leg
`PRZ_MATCH` is structurally impossible under v1. This v2 asks a **different**
question: does a *previously tested* projected PRZ, followed by a close beyond
X and a later same-leg pole signal, describe a useful historical context?
It is not a Gartley reversal, an entry rule, or a profitability result.

## Frozen, causal convention

| Step | Required evidence | Decision timestamp |
|---|---|---|
| 1 | Four consecutive confirmed PnF pivots X/A/B/C satisfy the unchanged restricted standard Gartley projection; zone is not late | C confirmation close |
| 2 | Strict B violation and full far-edge PRZ test, using completed 1m closes | Their observed closes |
| 3 | Closed price crosses Gartley X; v1 records `X_CROSSED_BEFORE_SIGNAL`, and the Gartley remains invalid | X-cross close |
| 4 | The immediately connected PnF leg emits the original ≥50% Du Plessis pole signal | Later pole decision close |

Require `zone_known_at < B_violation_at ≤ full_zone_test_at < X_cross_at
< pole_signal_at`. An expiry at the same close as a pole is evaluated after
the pole; a later signal cannot revive that candidate. One context match per
candidate. The v2 label `CONTEXT_MATCH` never replaces a v1 `PRZ_MATCH` or
the original pole event. No later close, trade outcome or fill is a feature.

Mechanically, this is a *failed Gartley followed by a pole*, not a Carney
pattern claim. There is no empirical tuning, alternative Fibonacci ratio,
change to consolidation, X invalidation, pole breakout, or protected baseline.
The v2 observer is default OFF and has no execution, database, UI, alerts,
trader, or network capability. The bounded CLI replays only up to 10,000
contiguous research candles from a SHA-pinned CSV and writes a new JSON file.
It records signal annotations and explicitly sets performance to
`NOT_EVALUATED`. It neither removes trades from the old replay nor produces
a filtered portfolio simulation.

The 2024 BTCUSDT data and pole outcomes have already been observed. Any
frequency on that year would be exploratory. A future empirical comparison
must first freeze an independent out-of-sample interval, cost/funding and fill
assumptions, blocked-signal accounting, metrics and an acceptance criterion.
The full 2024 trade/event ledger remains unavailable locally in this repo.

**Implementation gate:** targeted offline chronology and baseline-isolation
tests. **Empirical gate:** BLOCKED; no net-profitability claim or live promotion.
