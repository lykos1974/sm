# Project Workflow (Trading Research Lab)

## Research-Lab Operating Model
This project is managed as a disciplined trading research lab, where reproducibility and safe rollback take priority over ad-hoc iteration.

Pipeline intent:
`collector -> DB -> pnf_engine -> structure_engine -> strategy_engine -> strategy_validation -> export/stats`

Engine responsibilities are intentionally separated:
- `pnf_engine.py`: PnF construction only
- `structure_engine.py`: structural truth only
- `strategy_engine.py`: setup selection / promotion logic only
- `strategy_validation.py`: trade activation / lifecycle / resolution only
- `strategy_historical_backfill.py`: orchestration only
- `strategy_trade_export.py`: reporting / analytics only

## Stable Baseline vs Experiment Branches
- **Stable baseline branch/state**: current profitable rollback point; must remain reliable.
- **Experiment branches**: isolated test branches for one hypothesis each.
- Baseline is only replaced after explicit promotion under acceptance rules.

## Standard Experiment Flow
1. Start from stable baseline.
2. Create isolated branch.
3. Change one idea only.
4. Run backfill/export.
5. Record metrics.
6. Compare against acceptance rules.
7. Decide: **PROMOTE** / **KEEP AS RESEARCH** / **DISCARD**.

## Minimal Recommended Git Workflow
1. Sync local baseline branch.
2. Create branch with focused name (example: `exp/long-retest-filter-adjustment`).
3. Commit small, reversible changes with clear messages.
4. Run backfill/export and document scorecard metrics.
5. Open PR with decision and rationale.
6. Merge only when promotion criteria are met.

## What Not To Do
- Do not mix multiple ideas in one experiment.
- Do not judge success by win rate alone.
- Do not overwrite stable baseline with experimental logic.
