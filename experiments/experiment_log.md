# Experiment Log

## Stable Baseline Reference (Current Rollback Point)

Baseline profile:
- Direction: LONG only
- `breakout_context`: `POST_BREAKOUT_PULLBACK`
- `pullback_quality`: `HEALTHY`
- `active_leg_boxes`: 2
- Non-extended only
- All symbols in scope
- No early breakeven logic

Baseline metrics:
- `candidate_rows_registered`: 98
- `resolved_rows`: 89
- `win_rate_non_ambiguous`: 0.3146
- `avg_realized_r_multiple`: 0.1919
- `total_realized_r_multiple`: 17.0763
- `TP1 -> TP2 conversion`: 0.9286

Status: **Stable / profitable rollback point**.

---

## Reusable Experiment Entry Template

### Experiment ID
- ID:
- Date:
- Branch:
- Owner:

### Hypothesis (One Idea Only)
- Summary:
- Intended impact:

### Change Scope
- Files touched:
- Strategy behavior change:
- Schema change: Yes/No (if yes, justify)

### Scorecard
- `candidate_rows_registered`:
- `resolved_rows`:
- `win_rate_non_ambiguous`:
- `avg_realized_r_multiple`:
- `total_realized_r_multiple`:
- `TP1 -> TP2 conversion`:

### Decision
- Outcome: **PROMOTE / KEEP AS RESEARCH / DISCARD**
- Rationale:
- Follow-up:

---

## Historical Notes (Known Outcomes)
- Long baseline: **Stable / profitable rollback point**.
- BE at 1.5R: **DISCARD**.
- Short continuation mirror: **DISCARD**.
- Mixed long + short reversal: **KEEP AS RESEARCH**.
