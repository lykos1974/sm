# Trade Snapshot integration

## Current limitation

Your current validation store persists:
- raw_setup_json
- raw_structure_json

It does not persist:
- full historical PnF columns for each setup
- snapshot_path
- active/current column index

So old trades cannot be rebuilt as exact historical chart snapshots from the current DB alone.

## Recommended path for future trades

### 1) Add DB columns in strategy_validation.py

```sql
snapshot_path TEXT
active_column_index INTEGER
```

### 2) Extend register_setup(...)

Add optional args:
- snapshot_path
- active_column_index

and save them into strategy_setups.

### 3) Capture the PNG at setup creation time

Call the helper from scanner/backfill where you already have:
- engine.columns
- profile.box_size
- setup
- structure_state

Example:

```python
from strategy_snapshot_support import render_trade_snapshot_png, build_snapshot_filename

snapshot_path = build_snapshot_filename(symbol, setup_id)

render_trade_snapshot_png(
    symbol=symbol,
    side=setup["side"],
    setup_id=setup_id,
    columns=engine.columns,
    box_size=profile.box_size,
    entry=setup.get("ideal_entry"),
    sl=setup.get("invalidation"),
    tp1=setup.get("tp1"),
    tp2=setup.get("tp2"),
    support_level=structure_state.get("support_level"),
    resistance_level=structure_state.get("resistance_level"),
    active_column_index=structure_state.get("current_column_index"),
    title_note=str(setup.get("reason") or ""),
    output_path=snapshot_path,
)
```

### 4) Save current_column_index
Also persist current_column_index inside structure_state and strategy_setups.

### 5) Add “Open Snapshot” in the inspect UI
Read snapshot_path from DB and open with os.startfile(snapshot_path).

## Why this is the right intermediate step

- Stats app stays analytics-focused
- Snapshot becomes true historical visual evidence
- Scanner overlay can come later if still needed
