# Full 7-Market P+2 Causal Pole/Reversal Motif Audit

Date: 2026-06-03

## Objective

Determine whether the pole edge originates from an earlier causal motif, knowable at P+2, instead of only from the later look-ahead/non-causal core motif definition.

## Guardrails

- Research only.
- No production strategy changes.
- No live-trader changes.
- Do not promote this motif from this result.
- Use the same seven-market research universe as the validated historical execution model: `BTC`, `ETH`, `SOL`, `ENA`, `HYPE`, `SUI`, and `TAO`.
- Use the same execution assumptions: fixed three-box stop, fixed 2.5R target, break-even after +2R, and deduplicated opportunity execution.
- The fields `opposing_pole_distance_columns` and `enhanced_by_opposing_pole` are intentionally ignored for P+2 row selection.

## Causal P+2 Motif Definition

The audited motif is `CAUSAL_P2_POLE_REVERSAL_CONFIRMATION`:

1. P: a detected pole column exists.
2. P+1: its reversal column exists.
3. P+2: the immediate confirmation column exists.
4. Entry: first candle open after the P+2 confirmation column start timestamp.
5. Stop: fixed three-box stop.
6. Target: fixed 2.5R.
7. Management: break-even after +2R.

## Required Full-Audit Output

The audit emits a break-even execution row containing exactly the requested fields:

| Field | Meaning |
|---|---|
| `trades` | Trade count after deduplicating executable opportunities. |
| `wins` | TARGET_FIRST outcomes. |
| `losses` | STOP_FIRST outcomes. |
| `break_even_exits` | BREAK_EVEN_EXIT outcomes after +2R arming. |
| `win_rate` | Wins divided by trades. |
| `expectancy` | Realized R divided by trades. |
| `total_R` | Total realized R. |

## Direct Comparison Baselines

| Baseline | Trade count | Expectancy |
|---|---:|---:|
| Historical non-causal core motif | 460 | +1.654R/trade |
| Causal P+4 true-birth revalidation | — | -0.714R/trade |

The generated `p2_causal_motif_comparison.csv` compares the full seven-market P+2 expectancy directly against both baselines.

## Reproducibility Notes

The repeatable audit implementation lives in `research_v2/patterns/pole_p2_causal_motif_audit.py`. By default, the CLI now rejects partial symbol universes so local five-symbol diagnostic runs cannot be mistaken for the requested full seven-market audit. Test-only or exploratory partial runs require the explicit `--allow-partial-universe` override.
