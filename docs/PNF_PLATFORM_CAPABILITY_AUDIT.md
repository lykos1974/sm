# PnF Platform Capability Audit

Documentation-only audit of what the current PnF platform already computes versus what it currently displays. This document is intended to guide SaaS/UI roadmap planning without changing trading logic, UI behavior, database schema, or protected interfaces.

## Scope inspected

- `pnf_mvp/app.py`
- `pnf_mvp/pnf_engine.py`
- `pnf_mvp/structure_engine.py`
- `pnf_mvp/strategy_engine.py`
- `live_binance_forward_trader.py`
- `docs/PNF_SAAS_PLATFORM_ARCHITECTURE.md`

No code changes are proposed in this audit.

## 1. Current UI capabilities

### Scanner table

The Tkinter MVP provides a left-side scanner table with these displayed columns:

- Exchange
- Native symbol
- Market state
- Latest signal
- Priority
- Last price
- Score
- Updated time

The table is populated from in-memory scanner snapshots and sorted by priority, then score, then symbol. It supports selecting a symbol to redraw the chart and update the active symbol.

### PnF chart

The main canvas renders the selected symbol's PnF columns directly from `PnFColumn.levels(...)`:

- `X` columns are drawn with blue X marks.
- `O` columns are drawn with red circles.
- Both left and right price axes are rendered.
- Horizontal and vertical grid lines are rendered.
- Vertical and horizontal scrolling are supported.
- Mouse-drag panning and mouse-wheel scrolling are supported.
- The chart can center on the latest active area.

The chart header displays the selected symbol plus a compact metadata line:

- State
- Signal
- Last
- Score
- Priority

### Refresh behavior

The application bootstraps once from the database and then runs incremental refreshes every `REFRESH_MS = 3000` milliseconds. A manual refresh button triggers the same incremental refresh path if no refresh is already running and bootstrap is complete.

Refresh behavior currently:

- Loads cached PnF state when available.
- Applies new closed candles only.
- Rebuilds full history when cache is unavailable or when the user clicks `Rebuild selected`.
- Saves engine state, columns, scanner snapshots, and signals back to storage.
- Keeps user-panned views stable where possible.

### Exchange filter

The scanner includes an exchange filter with three values:

- `ALL`
- `BINANCE`
- `MEXC_FUT`

Changing the filter refreshes the table, updates the active symbol if the current symbol is not available in the filtered list, redraws the chart, and updates profile metadata.

### Signal panel

The bottom notebook contains a `Signals` tab. Filter-passing signal alerts are prepended to this panel and the newest alert is highlighted in red. Alerts include timestamp, symbol, signal type, trigger, priority, score, market state, and note.

The same alert path can also:

- Play a local alarm sound on Windows.
- Show a popup alert.
- Send a Telegram message when enabled.

### Log panel

The bottom notebook also contains a `Log` tab. It records bootstrap, refresh, rebuild, structure debug, strategy debug, export, and error messages. The UI includes a `Copy log` button and Control-C binding for copying the full log text.

### Structure debug button

The UI includes:

- A `Structure debug` checkbox.
- A symbol combobox for the debug target.
- A `Show structure now` button.

When enabled and triggered, the app computes `build_structure_state(...)` for the selected debug symbol and logs all returned structure fields. It also evaluates long and short pullback/retest strategy setups and logs a compact strategy line when available.

Important distinction: structure and strategy details are available only as log/debug output today, not as persistent visual panels or chart overlays.

### Export columns CSV

The `Export selected columns CSV` button writes the selected symbol's current PnF columns to a CSV file with the columns:

```text
idx,kind,top,bottom,start_ts,end_ts
```

This is currently a PnF-column export only. It does not export structure state, strategy setup fields, validation fields, live trade fields, or rendered chart annotations.

## 2. Current computed-but-not-visualized fields

| Field / Feature | Computed today? | Displayed in UI? | Source module/function | Notes |
|---|---:|---:|---|---|
| PnF columns | Yes | Yes | `pnf_mvp/pnf_engine.py` / `PnFEngine.update_from_price(...)`, `PnFColumn.levels(...)`; `pnf_mvp/app.py` / `_draw_selected(...)` | Rendered as X/O chart and exportable to CSV. |
| latest signal | Yes | Yes | `PnFEngine.latest_signal_name(...)`; `App._build_snapshot(...)`; `App._draw_selected(...)` | Displayed in scanner table and chart metadata as `BUY`, `SELL`, or `NONE`; detailed signal objects appear in alerts. |
| market_state | Yes | Yes | `PnFEngine.market_state(...)`; `App._build_snapshot(...)` | Displayed as scanner `STATE` and chart metadata. |
| score | Yes | Yes | `PnFEngine.score(...)`; `App._build_snapshot(...)` | Displayed in scanner and chart metadata. |
| priority | Yes | Yes | `App._priority_from_snapshot(...)`; `App._build_snapshot(...)` | UI-derived scanner priority based on signal/state/score. |
| trend_state | Yes | Debug/log only | `pnf_mvp/structure_engine.py` / `build_structure_state(...)` | Not shown in scanner, chart header, chart overlay, or dedicated panel. |
| trend_regime | Yes | Debug/log only | `build_structure_state(...)` | Computed as a softer regime field; currently visible only through structure debug logs. |
| immediate_slope | Yes | Debug/log only | `build_structure_state(...)` | Computed for structural context; not visualized. |
| swing_direction | Yes | Debug/log only | `build_structure_state(...)` | Not visualized. |
| support_level | Yes | Debug/log only | `build_structure_state(...)` | Good candidate for immediate chart overlay. |
| resistance_level | Yes | Debug/log only | `build_structure_state(...)` | Good candidate for immediate chart overlay. |
| breakout_context | Yes | Debug/log only | `build_structure_state(...)`; propagated into `strategy_engine._base_result(...)` | Important baseline discriminator; not shown as UI label today except logs. |
| is_extended_move | Yes | Debug/log only | `build_structure_state(...)` | Important guardrail/risk context; not visualized. |
| active_leg_boxes | Yes | Debug/log only | `build_structure_state(...)` | Important baseline discriminator; not visualized. |
| last_meaningful_x_high | Yes | No | `build_structure_state(...)` | Returned in structure state, but the debug text helper omits it; support/resistance cover related levels. |
| last_meaningful_o_low | Yes | No | `build_structure_state(...)` | Returned in structure state, but the debug text helper omits it; support/resistance cover related levels. |
| strategy status | Yes | Debug/log only | `pnf_mvp/strategy_engine.py` / `evaluate_pullback_retest_long(...)`, `evaluate_pullback_retest_short(...)`; `App._log_strategy_if_available(...)` | Logged as `REJECT`, `WATCH`, or `CANDIDATE`; registered for validation when eligible. |
| setup side | Yes | Debug/log only | `strategy_engine._base_result(...)` | Long and short setup evaluation both exist; short is experimental per engine comments. |
| ideal_entry | Yes | Debug/log only | `strategy_engine._base_result(...)` | Logged as `entry=...`; not plotted. |
| invalidation / stop | Yes | Debug/log only | `strategy_engine._base_result(...)` | Logged as `sl=...`; not plotted. |
| tp1 | Yes | Debug/log only | `strategy_engine._base_result(...)`; live trader `TriangleSignal` / trade tables | Strategy TP1 is logged; live trader also stores `tp1_price`. Not plotted. |
| tp2 | Yes | Debug/log only | `strategy_engine._base_result(...)`; live trader `TriangleSignal` / trade tables | Strategy TP2 is logged; live trader also stores `tp2_price`. Not plotted. |
| rr1 | Yes | Debug/log only | `strategy_engine._base_result(...)` | Logged in strategy debug only. |
| rr2 | Yes | Debug/log only | `strategy_engine._base_result(...)` | Logged in strategy debug only. |
| quality_score | Yes | Debug/log only | `strategy_engine._base_result(...)` | Logged in strategy debug only. |
| quality_grade | Yes | Debug/log only | `strategy_engine._base_result(...)` | Good candidate for setup quality badge. |
| reject_reason | Yes | Debug/log only | `strategy_engine._base_result(...)` | Useful for explaining rejected setups; currently log-only. |
| pullback_quality | Yes | Debug/log only | `strategy_engine._base_result(...)` | Baseline-critical field; log-only. |
| risk_quality | Yes | Debug/log only | `strategy_engine._base_result(...)` | Log-only. |
| reward_quality | Yes | Debug/log only | `strategy_engine._base_result(...)` | Log-only. |
| live trade entry | Yes | No | `live_binance_forward_trader.py` / `TriangleSignal.entry_price`, `live_trades_binance.entry_price` | Live trader is independent from UI; no trade markers or journal view in Tkinter. |
| live trade stop | Yes | No | `TriangleSignal.stop_price`, `live_trades_binance.stop_price`, protective order builder | Computed/stored by live forward trader; not consumed by UI. |
| live trade TP | Yes | No | `TriangleSignal.tp1_price`, `TriangleSignal.tp2_price`, `live_trades_binance.tp1_price`, `tp2_price` | Computed/stored by live forward trader; not consumed by UI. |
| realized_r | Yes | No | `live_signals_binance.realized_r`, `live_trades_binance.realized_r`, journal export columns | Trade outcome field exists in live trader tables/export, not UI. |
| trade status | Yes | No | `live_trades_binance.status`, `TRADE_JOURNAL_COLUMNS.status`, lifecycle update functions | Live-trade lifecycle exists outside Tkinter display. |

## 3. Gap analysis

### A. Easy UI overlay

These are low-risk because the underlying values are already computed by the structure engine and can be consumed read-only by the current UI:

- **Support/resistance overlays**: draw horizontal lines at `support_level` and `resistance_level` on the existing Tkinter PnF canvas.
- **Trend state label**: add `trend_state` to a structure panel or chart metadata label.
- **Breakout context label**: add `breakout_context` to a structure panel or chart metadata label.

Implementation risk: low, provided the UI calls `build_structure_state(...)` and does not reimplement structure logic.

### B. Medium UI overlay

These require more UI layout and chart annotation care, but still consume existing strategy output:

- **Entry/stop/TP levels**: plot `ideal_entry`, `invalidation`, `tp1`, and `tp2` as labeled price-level overlays.
- **Setup zones**: visualize `zone_low` to `zone_high` as a shaded or bounded region.
- **Strategy quality badge**: display `status`, `quality_grade`, `quality_score`, `pullback_quality`, `risk_quality`, `reward_quality`, and `reject_reason` in a setup card.

Implementation risk: medium because the UI must decide which side/setup to display when long and short evaluations both exist. It should remain read-only and explicitly label short-side setups as experimental unless promoted.

### C. Larger platform work

These require new persistence, API, account, browser, or product work beyond a simple Tkinter visualization PR:

- **Trade markers on chart**: requires joining chart columns/time/price with live or journal trade records, plus marker lifecycle rules.
- **Multi-user watchlists**: requires user state, watchlist persistence, and likely SaaS account boundaries.
- **Web chart**: requires a browser rendering layer and read-only API over existing PnF columns and derived state.
- **Real-time API**: requires serving derived fields safely, versioning output, and avoiding accidental trading-side coupling.
- **Subscription gating**: requires identity, billing provider state, plan entitlements, and access checks.

The SaaS architecture document already frames these as separate platform/data concerns, including derived PnF output storage, setup records, trade records, user watchlists, and subscription data.

## 4. Recommended next UI phases

### Phase UI-1: Structure panel and support/resistance overlays

Add a read-only structure panel for the selected symbol and draw support/resistance overlays on the existing Tkinter PnF chart.

Minimum fields:

- `trend_state`
- `trend_regime`
- `immediate_slope`
- `swing_direction`
- `support_level`
- `resistance_level`
- `breakout_context`
- `is_extended_move`
- `active_leg_boxes`

Why first:

- It exposes already-computed structure without changing trading logic.
- It makes the current chart more useful immediately.
- It creates UI conventions for later strategy overlays.

### Phase UI-2: Strategy setup card with entry/SL/TP/RR

Add a selected-symbol setup card that consumes `evaluate_pullback_retest_long(...)` and `evaluate_pullback_retest_short(...)` outputs.

Minimum fields:

- Side
- Status
- Zone low/high
- Ideal entry
- Invalidation / stop
- TP1 / TP2
- RR1 / RR2
- Quality score / grade
- Pullback, risk, and reward quality
- Reject reason / reason

The first version should display rather than plot these levels. Chart plotting can follow once labeling and side-selection rules are clear.

### Phase UI-3: Trade journal and trade markers

Add a read-only trade journal view after the setup card exists.

Minimum fields:

- Trade ID
- Symbol
- Pattern/strategy
- Side
- Entry
- Stop
- Targets
- Status
- Exit price/time
- Realized R
- Fees
- Notes

Only after journal display is stable should trade markers be plotted on the chart.

### Phase UI-4: Prototype web chart using existing PnF columns

Prototype a browser chart that consumes existing PnF columns and derived structure/setup fields through read-only endpoints.

The web prototype should initially avoid:

- Execution controls
- User-editable strategy logic
- Subscription gating complexity
- Live-trader coupling

## 5. Guardrails

- Do not move logic into UI.
- UI only consumes already-computed fields.
- Structure/strategy engine remain source of truth.
- Live trader remains independent from UI.
- SaaS frontend must be read-only initially.
- Do not change exported metric names.
- Do not change protected strategy or validation interfaces without explicit approval.
- Keep long-only profitable baseline behavior stable unless a future experiment is explicitly promoted.
- Treat short-side output as experimental unless explicitly promoted.

## 6. Final recommendation

The first implementation PR after this audit should add structure/strategy visualization to the existing Tkinter UI first, before building full web SaaS.

Recommended PR scope:

1. Add a read-only selected-symbol structure panel populated from `build_structure_state(...)`.
2. Draw support and resistance overlays on the current Tkinter PnF canvas.
3. Add a compact read-only strategy setup card that displays existing long/short setup outputs, with short-side output clearly labeled experimental.
4. Do not alter `PnFEngine`, `structure_engine`, `strategy_engine`, validation storage, live trading, or database schema.

This is the safest next step because it converts computed-but-hidden platform intelligence into visible UI value while preserving the current research baseline and avoiding premature SaaS infrastructure work.
