# PnF Research Checkpoint

## 1) Current stable technical state

- `incremental_fast` exists and removes the `build_structure_state` bottleneck.
- v4 diagnostics/export fields exist.
- A research result log exists.
- Validation/backfill experiments were run.

## 2) What was proven

- The `build_structure_state` bottleneck is solved.
- Unlimited WATCH registration is not viable.
- `max_watch=20` keeps pending bounded but is still not clearly profitable.
- Event/pending database writes remain a performance bottleneck.
- No final strategy edge has been confirmed yet.

## 3) What was disproven / rejected

- Blind EARLY -> WATCH expansion.
- Unlimited multiple trades.
- Assuming CANDIDATE baseline is automatically profitable.
- Assuming `active_leg_boxes=5` is proven without matching trade-export validation.

## 4) Most important latest metrics

Capped WATCH run:

- `validation_rows=538`
- `resolved=518`
- `max_pending=20`
- `avg_pending=19.8531`
- `elapsed_update_pending_s=330.94`
- `avg_realized_r_multiple=-0.0208`
- `total_realized_r_multiple=-10.7970`
- `LONG avg R=+0.4473`
- `SHORT avg R=-0.1925`
- `LATE_EXTENSION avg R=+0.3763`
- `LONG_LATE_EXTENSION__BULLISH_REGIME__EXTENDED: 15 trades, avg R=+1.6667`
- `HEALTHY_GEOMETRY: 13 trades, avg R=+1.7692`

## 5) Current risk

- We are at risk of overfitting and policy churn.
- No more strategy/policy implementation should be made without a written experiment plan.

## 6) Recommended freeze decision

- Stop development temporarily.
- Do not merge more strategy/validation behavior changes.
- Keep current state as a research checkpoint.
- Next step should be analysis-only: define one controlled experiment with clear success criteria before any code.

## 7) Open questions

- What validation policy should be treated as baseline?
- Should WATCH be analyzed as research-only rather than registered trades?
- Should candidate/WATCH selection be redefined only after larger multi-symbol evidence?
- Should event-only persistence be implemented later as performance infrastructure, separate from strategy?
