# Incremental Structure Engine Design (Planning Only)

## Scope / Non-Goals

This document is a **design-only** deliverable.

- No strategy logic changes.
- No output schema changes.
- No optimization/refactor implementation in this phase.
- Goal: define a safe, testable path to replace repeated full recomputation in `build_structure_state(...)` with an incremental equivalent later.

---

## 1) Current `build_structure_state()` behavior

## Where it is called

`strategy_historical_backfill.py` calls:

1. `engine.update_from_price(...)`
2. `build_structure_state(...)` (via `evaluate_setups`)
3. strategy evaluators (`evaluate_pullback_retest_long/short`)

So structure generation executes once per candle per symbol, making its full-list scans the dominant hot path.

## Inputs currently passed

`build_structure_state(symbol, profile, columns, latest_signal_name, market_state, last_price, config=None)`

- `symbol`: current symbol string
- `profile`: provides `box_size`
- `columns`: full PnF column list (`engine.columns`)
- `latest_signal_name`: from `engine.latest_signal_name()`
- `market_state`: from `engine.market_state()`
- `last_price`: from `engine.last_price`
- `config`: optional `StructureConfig`

## Outputs currently produced

Returned structure dict fields:

- `symbol`
- `trend_state`
- `trend_regime`
- `immediate_slope`
- `swing_direction`
- `support_level`
- `resistance_level`
- `breakout_context`
- `is_extended_move`
- `active_leg_boxes`
- `impulse_boxes`
- `pullback_boxes`
- `impulse_to_pullback_ratio`
- `last_meaningful_x_high`
- `last_meaningful_o_low`
- `current_column_kind`
- `current_column_top`
- `current_column_bottom`
- `latest_signal_name`
- `market_state`
- `last_price`
- `notes`

If `columns` is empty, it returns EARLY/empty defaults.

## Why expensive today

`build_structure_state(...)` itself invokes helper chains that repeatedly scan `columns` or `columns[:-1]`:

- `_meaningful_x_columns`, `_meaningful_o_columns` (full pass)
- `_meaningful_x_highs`, `_meaningful_o_lows` (full pass + projection)
- `_last_two_meaningful_x_highs`, `_last_two_meaningful_o_lows` (full pass then slice)
- `_last_meaningful_x_high`, `_last_meaningful_o_low` (full pass)
- `_last_of_kind(..., before_index=...)` (reverse scan)
- `_recent_direction_bias` (windowed scan on completed columns)
- trend/swing/breakout helpers each calling the above again

Because these are re-run every candle, effective cost is repeated O(N) work on growing column history.

---

## 2) Field dependency map

Legend:

- **Direct source**: helper/function or direct passthrough
- **PnF dependency**: what part of column history is needed
- **Incremental?**: can be maintained with event updates
- **Current mode**: full recompute vs local lookup

| Output field | Direct source today | PnF column dependency | Incremental? | Current mode |
|---|---|---|---|---|
| `trend_state` | `_detect_trend_state(...)` | Last two meaningful X highs + O lows, swing, regime, market_state | Yes | Recompute with repeated scans |
| `trend_regime` | `_detect_trend_regime(...)` | Last two meaningful highs/lows, recent direction bias window, last meaningful S/R, current col | Yes | Recompute with repeated scans |
| `immediate_slope` | `_detect_immediate_slope(...)` | Current column kind + trend outputs | Yes | Local compute |
| `swing_direction` | `_detect_swing_direction(...)` | Last two meaningful X highs + O lows, fallback last completed col | Yes | Recompute with repeated scans |
| `support_level` | `_last_meaningful_o_low(...)` | Last completed O low | Yes | Full meaningful O scan |
| `resistance_level` | `_last_meaningful_x_high(...)` | Last completed X high | Yes | Full meaningful X scan |
| `breakout_context` | `_detect_breakout_context(...)` | Current col, previous same-kind completed col, regime, active leg boxes | Yes | Includes reverse scans |
| `is_extended_move` | `active_leg_boxes >= threshold` | Current column span / box size | Yes | Local compute |
| `active_leg_boxes` | `_active_leg_boxes(...)` | Current column top/bottom | Yes | Local compute |
| `current_column_kind` | `_col_kind(current)` | Current column | Yes | Local compute |
| `current_column_top` | `_col_top(current)` | Current column | Yes | Local compute |
| `current_column_bottom` | `_col_bottom(current)` | Current column | Yes | Local compute |
| `last_meaningful_x_high` | `_last_meaningful_x_high(...)` | Last completed X top | Yes | Full meaningful X scan |
| `last_meaningful_o_low` | `_last_meaningful_o_low(...)` | Last completed O bottom | Yes | Full meaningful O scan |
| `latest_signal_name` | passthrough input | Engine signal state | Yes (external) | Passthrough |
| `market_state` | passthrough input | Engine state | Yes (external) | Passthrough |
| `last_price` | passthrough input | Engine state | Yes (external) | Passthrough |
| `impulse_boxes` | `_column_span_boxes(prev_x)` under context guard | Previous completed X col + box_size | Yes | Reverse lookup each call |
| `pullback_boxes` | `_column_span_boxes(current)` under context guard | Current O col + box_size | Yes | Local compute |
| `impulse_to_pullback_ratio` | `_safe_ratio(...)` | `impulse_boxes`, `pullback_boxes` | Yes | Local compute |
| `notes` | `_build_notes(...)` | Derived from already computed outputs | Yes | Local compute |

Notes:
- “Meaningful” columns are currently defined as **completed columns only** (`columns[:-1]`).
- Any incremental engine must preserve that exact semantic boundary.

---

## 3) Proposed incremental architecture

Introduce a first-class `IncrementalStructureState` that is updated by PnF events and can emit a snapshot identical to current `build_structure_state(...)` output.

## Core object

`IncrementalStructureState` (per symbol/profile):

- Immutable config mirror (`StructureConfig`, `box_size`)
- Cached rolling values:
  - `current_column_kind/top/bottom`
  - `active_leg_boxes`, `is_extended_move`
  - last completed X/O references
  - last two meaningful X highs / O lows
  - rolling recent-direction bias window over completed columns
  - cached `swing_direction`, `trend_regime`, `trend_state`, `immediate_slope`, `breakout_context`
  - cached impulse/pullback/ratio fields
- Input passthrough cache:
  - `latest_signal_name`, `market_state`, `last_price`

## Event model

State updates from four canonical PnF events:

1. **Column extension** (same column kind, top/bottom changed)
   - Update current column metrics only.
   - Re-evaluate extension/breakout context that depends on current span and previous same-kind references.

2. **Column reversal** (new column appended)
   - Promote prior current column to completed-history view.
   - Update last completed X/O pointers and last-two highs/lows caches.
   - Update recent-bias rolling window.
   - Recompute dependent trend/swing/regime fields using caches.

3. **Breakout/breakdown signal update**
   - Update passthrough `latest_signal_name`/`market_state` and any trend fallback dependent on market state.

4. **New meaningful support/resistance availability**
   - Triggered implicitly on reversal finalization of X/O columns.
   - Update `support_level` / `resistance_level` caches.

## Snapshot contract

`snapshot(...) -> Dict[str, Any]` returns exactly the current structure dict schema/values, including:

- same field names
- same enum strings
- same `None` behavior
- same “meaningful == completed columns” interpretation
- same note generation format

---

## 4) Compatibility plan (preserve exact outputs)

To preserve behavior exactly:

1. Keep existing `build_structure_state(...)` as reference implementation.
2. Define `IncrementalStructureState.snapshot()` output schema as strictly identical.
3. Preserve ordering/precedence in decision logic:
   - trend and regime fallbacks
   - breakout context priority
   - extension threshold handling
4. Preserve floating behavior and rounding assumptions (especially box-span-derived values).
5. Preserve empty/early behavior for short histories.
6. Preserve current dependency on external inputs (`latest_signal_name`, `market_state`, `last_price`) as passthrough values.

---

## 5) Dual-run equivalence harness design

Design a validator that runs both implementations side-by-side at every candle:

```python
old = build_structure_state(...)
new = incremental_structure_state.snapshot(...)
assert normalize(old) == normalize(new)
```

## Harness requirements

- Run over full historical dataset.
- Run across multiple symbols/profiles.
- Compare at **every candle**, not only final state.
- Report first divergence with:
  - symbol
  - candle timestamp/index
  - current and previous columns context
  - field-level diff

## Coverage scenarios

Must include cases with:

- Frequent reversals
- Long same-direction extensions
- Fake breakouts / quick invalidations
- Early short-history periods (len < thresholds)
- Low-volatility / range regimes
- High-volatility transitions

## Normalization rules for comparison

- Exact equality for enums/strings/booleans/None.
- Numeric tolerance only where unavoidable for float representation (very small epsilon).
- Notes list compared exactly (ordering and text), since this is part of output contract.

---

## 6) Migration plan

## Phase 1 — dependency map + tests

- Finalize field-level dependency matrix (this doc).
- Add focused unit tests for current `build_structure_state(...)` behavior and edge semantics.

## Phase 2 — dual-run harness

- Implement offline comparison harness using existing full recompute as source of truth.
- Ensure deterministic reproducible runs and artifacted diff reports.

## Phase 3 — incremental state prototype

- Implement `IncrementalStructureState` with no production wiring.
- Feed it from replayed PnF events/candle progression.

## Phase 4 — shadow mode comparison

- Wire into backfill in shadow mode:
  - old output still used for strategy
  - new output computed and compared only
- Track mismatch metrics and field divergence frequency.

## Phase 5 — controlled replacement

- Replace `build_structure_state` usage only after sustained zero-diff (or formally approved tolerance for numeric-only noise).
- Keep fallback switch to old path during stabilization window.

---

## 7) Risks (silent behavior drift)

1. **Meaningful-column boundary drift**
   - Accidentally treating current active column as meaningful instead of completed-only.

2. **Fallback precedence drift**
   - Different ordering in trend/regime fallback checks can flip classifications.

3. **Breakout context edge drift**
   - Reversal timing vs previous same-kind lookup can shift fresh breakout vs pullback classification.

4. **Window/bias off-by-one errors**
   - Recent direction bias window must match current slice semantics exactly.

5. **Float handling differences**
   - Small numeric differences in span/ratio fields can alter downstream quality gating.

6. **External-input sync mismatch**
   - Snapshot may lag or lead `market_state/latest_signal_name/last_price` updates by one candle.

7. **Notes text drift**
   - Even if trading logic is unchanged, altered notes formatting can break exact-equality harness expectations.

8. **Initialization/early-state mismatches**
   - Incomplete handling of low column counts can bias early classification.

9. **Symbol/profile cross-contamination**
   - Reusing incremental cache across symbols by mistake can silently corrupt outputs.

---

## Immediate next step (still design-safe)

Build the dual-run harness first, then implement incremental state in shadow mode only. No strategy path should consume incremental output until equivalence is demonstrated.
