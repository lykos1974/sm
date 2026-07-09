# ROS-0 Proof of Concept — Pole Core Motif / Next Open Entry

## 1. Selected research family

**Selected family:** Pole Core Motif / Next Open Entry.

**Selection rationale:** This is the preferred and strongest migration candidate because the repository records a completed research family with observations, hypotheses, validation runs, evidence, decisions, failures, promotion discussions, rollback points, baseline comparison, and explicit research-only guardrails.

**Scope boundary:** This document is a ROS-0 historical representation only. It does not modify detectors, trading logic, strategy, execution, validation code, research code, datasets, exports, sqlite, parquet, or manifests.

**Primary source artifacts used:**

| Artifact ID | Repository artifact | Role in this migration |
|---|---|---|
| `A1` | `docs/research_results_log.md` | Authoritative high-level snapshot for the 2026-06-03 Pole Motif Execution Baseline Validation. |
| `A2` | `docs/p2_causal_motif_research.md` | Follow-on causal audit definition, guardrails, direct baselines, and reproducibility notes. |
| `A3` | `research_v2/patterns/pole_core_motif_next_open_expectancy_audit.py` | Research implementation for the Next Open expectancy audit. |
| `A4` | `research_v2/patterns/pole_core_motif_next_open_robustness_audit.py` | Research implementation for robustness falsification of the surviving Next Open candidate. |
| `A5` | `research_v2/patterns/pole_core_motif_execution_reality_audit.py` | Research implementation for execution-reality audit. |
| `A6` | `research_v2/patterns/pole_execution_model_v1_audit.py` | Research implementation for execution model v1. |
| `A7` | `research_v2/patterns/pole_be_research_audit.py` | Research implementation for break-even management audit. |
| `A8` | `research_v2/patterns/pole_portfolio_reality_audit.py` | Research implementation for portfolio reality / concentration. |
| `A9` | `research_v2/patterns/pole_baseline_stability_audit.py` | Research implementation for baseline stability. |
| `A10` | `research_v2/patterns/pole_p2_causal_motif_audit.py` | Research implementation for the causal P+2 audit. |
| `A11` | `research_v2/patterns/pole_causal_revalidation.py` | Research implementation for causal P+4 true-birth revalidation. |
| `A12` | `replay_summary.md` | Live replay parity summary present in repository. |

**Status classification key:**

| Classification | Meaning |
|---|---|
| `KNOWN` | The value is explicitly recorded in repository artifacts. |
| `UNKNOWN` | The value may exist but was not found in the inspected repository artifacts. |
| `NOT APPLICABLE` | The field does not apply to this object. |
| `LOST` | The value once existed but is no longer reconstructable from the repository evidence inspected here. |
| `NEVER RECORDED` | The inspected artifacts indicate no historical record was created for the field. |

---

## 2. Historical inventory

### 2.1 Family-level inventory

| Field | Value | Classification | Artifact reference |
|---|---|---|---|
| Research family | Pole Core Motif / Next Open Entry | `KNOWN` | `A1`, `A3`, `A4` |
| Research status | `VALIDATED_POSITIVE_RESEARCH_BASELINE / CONCENTRATED_EDGE` | `KNOWN` | `A1` |
| Production status | Research only; not production; not promoted | `KNOWN` | `A1`, `A2` |
| Entry | `NEXT_COLUMN_OPEN_ENTRY` | `KNOWN` | `A1`, `A2`, `A3`, `A4` |
| Stop | Fixed 3-box stop | `KNOWN` | `A1`, `A2` |
| Target | Fixed 2.5R | `KNOWN` | `A1`, `A2` |
| Management | Move stop to break-even after +2R | `KNOWN` | `A1`, `A2`, `A7` |
| TP1 / TP2 / trailing / scaling / pyramiding | None in fixed baseline | `KNOWN` | `A1` |
| Filters / optimization variants | None in fixed baseline | `KNOWN` | `A1` |
| Universe | Seven-market research universe: `BTC`, `ETH`, `SOL`, `ENA`, `HYPE`, `SUI`, `TAO` | `KNOWN` | `A2` |
| Market families | BINANCE and MEXC | `KNOWN` | `A1` |
| Earliest exact experiment date | 2026-06-03 snapshot date | `KNOWN` | `A1`, `A2` |
| Exact command lines for all historical runs | Not recorded in the high-level snapshot | `NEVER RECORDED` | `A1` |
| Export artifact paths for core validation chain | Recorded as paths under `pnf_mvp/exports/...` | `KNOWN` | `A1` |
| Export files themselves | Not present in inspected working tree at those paths | `LOST` | `A1` path references, working tree absence |
| Detector changes | None represented by this migration | `NOT APPLICABLE` | Scope boundary |
| Strategy changes | None represented by this migration | `NOT APPLICABLE` | Scope boundary |

### 2.2 Historical artifacts and ROS object anchors

| Historical artifact | ROS anchor | Recorded status | Classification |
|---|---|---:|---|
| Observation that a pole motif family existed and warranted execution validation | Observation | Present through research log and audit module names | `KNOWN` |
| Belief that the fixed core motif with Next Open execution had positive expectancy | Belief | Validated as positive research baseline | `KNOWN` |
| Next Open expectancy audit | ValidationRun | Implementation present; exact export not present | `KNOWN` / exports `LOST` |
| Execution reality audit | ValidationRun | Result `EDGE_SURVIVES` recorded | `KNOWN` |
| Execution model v1 audit | ValidationRun | Result `EXECUTION_READY` recorded | `KNOWN` |
| Break-even audit | ValidationRun | Result `BE_IMPROVES` recorded for +2R BE | `KNOWN` |
| Portfolio reality audit | ValidationRun | Result `PORTFOLIO_FRAGILE` due to `SYMBOL_CONCENTRATION` | `KNOWN` |
| Symbol dependence audit | ValidationRun | Result `EDGE_PARTIALLY_CONCENTRATED`, not BTC-dependent | `KNOWN` |
| Market-family correction audit | ValidationRun | Result `CONCENTRATED_EDGE` | `KNOWN` |
| P+2 causal motif audit | ValidationRun | Defined as research-only, no promotion | `KNOWN` |
| P+4 true-birth revalidation | ValidationRun | Baseline expectancy recorded as -0.714R/trade | `KNOWN` |
| Live replay parity summary | Evidence artifact | Present but zero generated/matched/missing/extra trades | `KNOWN` |

---

## 3. ROS mapping

### 3.1 Observation(s)

| ROS ID | Observation | Classification | Traceability |
|---|---|---|---|
| `OBS-POLE-001` | A pole/reversal/core motif family was identified as a candidate market structure worth validating. | `KNOWN` | Implied by `A1`, implemented by `A3`-`A11` |
| `OBS-POLE-002` | The fixed Next Open execution model produced a recorded positive research baseline. | `KNOWN` | `A1` |
| `OBS-POLE-003` | The edge survived multiple execution and stability audits but remained materially concentrated. | `KNOWN` | `A1` |
| `OBS-POLE-004` | BTC contributed about 60.8% of total R. | `KNOWN` | `A1` |
| `OBS-POLE-005` | BINANCE contributed about 81.5% of total R. | `KNOWN` | `A1` |
| `OBS-POLE-006` | Edge was positive in both BINANCE and MEXC market families. | `KNOWN` | `A1` |
| `OBS-POLE-007` | P+2 causal formulation was investigated to determine whether the edge was knowable earlier than the later non-causal core motif definition. | `KNOWN` | `A2`, `A10` |
| `OBS-POLE-008` | P+4 true-birth causal revalidation was a negative comparison baseline at -0.714R/trade. | `KNOWN` | `A2`, `A10`, `A11` |

### 3.2 Belief(s)

| ROS ID | Belief | Classification | Traceability |
|---|---|---|---|
| `BEL-POLE-001` | The Pole Core Motif can define a repeatable research family. | `KNOWN` | `A1`, `A3`-`A11` |
| `BEL-POLE-002` | `NEXT_COLUMN_OPEN_ENTRY` is an observable entry candidate that may preserve the motif edge. | `KNOWN` | `A1`, `A3`, `A4` |
| `BEL-POLE-003` | A fixed 3-box stop and fixed 2.5R target can express the edge without additional TP1/TP2/trailing/scaling/pyramiding/filter optimization. | `KNOWN` | `A1`, `A2` |
| `BEL-POLE-004` | Break-even after +2R improves the baseline. | `KNOWN` | `A1`, `A7` |
| `BEL-POLE-005` | The edge is not fully diversified because contribution concentration remains material. | `KNOWN` | `A1`, `A8`, `A9` |
| `BEL-POLE-006` | An earlier causal P+2 motif might explain or replace the non-causal core motif. | `KNOWN` | `A2`, `A10` |

### 3.3 Hypothesis

| ROS ID | Hypothesis | Expected evidence | Classification |
|---|---|---|---|
| `HYP-POLE-001` | If the Pole Core Motif is entered at `NEXT_COLUMN_OPEN_ENTRY` with a fixed 3-box stop and fixed 2.5R target, then the family should show positive expectancy in historical execution validation. | Trade count, wins, losses, win rate, expectancy, total R | `KNOWN` |
| `HYP-POLE-002` | If break-even is armed after +2R, then risk management should improve the fixed baseline. | Break-even exits, expectancy change, total R change | `KNOWN` |
| `HYP-POLE-003` | If the edge is robust, it should survive execution-reality, portfolio, symbol-dependence, and market-family checks. | Audit verdicts and concentration metrics | `KNOWN` |
| `HYP-POLE-004` | If the causal P+2 motif captures the true edge origin, then it should compare favorably against the 460-trade non-causal core baseline and the P+4 causal baseline. | P+2 expectancy, total R, comparison CSV | `KNOWN` |

### 3.4 ValidationRun(s)

| ROS ID | ValidationRun | Inputs / assumptions | Result | Classification |
|---|---|---|---|---|
| `VR-POLE-001` | Next Open expectancy audit | Existing chronology, `NEXT_COLUMN_OPEN_ENTRY`, fixed 3-box stop, fixed targets | Surviving Next Open candidate exists; detailed export missing from working tree | `KNOWN` / export `LOST` |
| `VR-POLE-002` | Execution reality audit | Fixed baseline candidate under execution reality | `EDGE_SURVIVES` | `KNOWN` |
| `VR-POLE-003` | Execution model v1 audit | Fixed 3-box stop, fixed 2.5R target, deduplicated opportunity execution | `EXECUTION_READY` | `KNOWN` |
| `VR-POLE-004` | Break-even audit | Break-even after +2R | `BE_IMPROVES` | `KNOWN` |
| `VR-POLE-005` | Portfolio reality audit | Fixed baseline across markets / portfolio constraints | `PORTFOLIO_FRAGILE` due to `SYMBOL_CONCENTRATION` | `KNOWN` |
| `VR-POLE-006` | Symbol dependence audit | Symbol contribution analysis | `EDGE_PARTIALLY_CONCENTRATED`, not BTC-dependent | `KNOWN` |
| `VR-POLE-007` | Market-family correction audit | BINANCE vs MEXC family comparison | `CONCENTRATED_EDGE`; positive in both families | `KNOWN` |
| `VR-POLE-008` | P+2 causal motif audit | P, P+1 reversal, P+2 confirmation, first candle open after P+2 confirmation, fixed 3-box stop, 2.5R target, BE after +2R | Research-only comparison; no promotion from result | `KNOWN` |
| `VR-POLE-009` | P+4 true-birth causal revalidation | Causal true-birth revalidation of core motif | -0.714R/trade comparison baseline | `KNOWN` |
| `VR-POLE-010` | Live replay parity | Replay summary present | 0 trades generated, 0 matching, 0 missing, 0 extra | `KNOWN` |

### 3.5 Evidence

| ROS ID | Evidence | Value | Classification | Source |
|---|---|---:|---|---|
| `EVD-POLE-001` | Trades | 460 | `KNOWN` | `A1`, `A2`, `A10` constants |
| `EVD-POLE-002` | Wins | 332 | `KNOWN` | `A1` |
| `EVD-POLE-003` | Losses | 69 | `KNOWN` | `A1` |
| `EVD-POLE-004` | Break-even exits | 59 | `KNOWN` | `A1` |
| `EVD-POLE-005` | Win rate | 72.17% | `KNOWN` | `A1` |
| `EVD-POLE-006` | BE rate | 12.83% | `KNOWN` | `A1` |
| `EVD-POLE-007` | Expectancy | +1.654R/trade | `KNOWN` | `A1`, `A2`, `A10` constants |
| `EVD-POLE-008` | Total R | +761R | `KNOWN` | `A1` |
| `EVD-POLE-009` | Max drawdown | 4R | `KNOWN` | `A1` |
| `EVD-POLE-010` | Longest losing streak | 4 | `KNOWN` | `A1` |
| `EVD-POLE-011` | Median concurrent positions | 1 | `KNOWN` | `A1` |
| `EVD-POLE-012` | Max concurrent positions | 2 | `KNOWN` | `A1` |
| `EVD-POLE-013` | Average active risk | ~1.03R | `KNOWN` | `A1` |
| `EVD-POLE-014` | Peak active risk | 2R | `KNOWN` | `A1` |
| `EVD-POLE-015` | BINANCE expectancy | +1.655R | `KNOWN` | `A1` |
| `EVD-POLE-016` | MEXC expectancy | +1.653R | `KNOWN` | `A1` |
| `EVD-POLE-017` | BTC contribution | ~60.8% of total R | `KNOWN` | `A1` |
| `EVD-POLE-018` | BINANCE contribution | ~81.5% of total R | `KNOWN` | `A1` |
| `EVD-POLE-019` | P+4 causal true-birth expectancy | -0.714R/trade | `KNOWN` | `A2`, `A10` |
| `EVD-POLE-020` | P+2 exact full-audit metrics | Not present in inspected docs; generated comparison CSV path is described but output values are not recorded in `A2` | `UNKNOWN` | `A2` |
| `EVD-POLE-021` | Replay parity trades generated / matching / missing / extra | 0 / 0 / 0 / 0 | `KNOWN` | `A12` |

### 3.6 Decision(s)

| ROS ID | Decision | Rationale | Classification |
|---|---|---|---|
| `DEC-POLE-001` | Treat the family as a validated positive research baseline. | Fixed baseline had +1.654R/trade, +761R total R, and survived execution audits. | `KNOWN` |
| `DEC-POLE-002` | Do not treat it as production. | Research log explicitly says not production and not promoted. | `KNOWN` |
| `DEC-POLE-003` | Preserve concentration warning. | BTC and BINANCE contribution concentration remained material. | `KNOWN` |
| `DEC-POLE-004` | Do not optimize further based only on the snapshot. | Research log warning states not to optimize further based only on the snapshot. | `KNOWN` |
| `DEC-POLE-005` | Do not promote P+2 motif from its result. | P+2 guardrail explicitly says not to promote this motif from this result. | `KNOWN` |
| `DEC-POLE-006` | Next phase should address concentration risk by expanding MEXC/non-BTC sample or adding concentration controls. | Research log recommendation. | `KNOWN` |

### 3.7 Knowledge

| ROS ID | Knowledge item | Classification | Traceability |
|---|---|---|---|
| `KNW-POLE-001` | The fixed research baseline is `NEXT_COLUMN_OPEN_ENTRY`, fixed 3-box stop, fixed 2.5R target, break-even after +2R, no TP1/TP2/trailing/scaling/pyramiding/filters/optimization variants. | `KNOWN` | `A1`, `A2` |
| `KNW-POLE-002` | The family is a positive research baseline, not production. | `KNOWN` | `A1` |
| `KNW-POLE-003` | The validated baseline is concentrated and not fully diversified. | `KNOWN` | `A1` |
| `KNW-POLE-004` | The edge is positive in both BINANCE and MEXC market families. | `KNOWN` | `A1` |
| `KNW-POLE-005` | Break-even after +2R is part of the fixed baseline because the break-even audit improved it. | `KNOWN` | `A1`, `A7` |
| `KNW-POLE-006` | P+4 true-birth causal revalidation did not preserve the non-causal core edge. | `KNOWN` | `A2`, `A11` |
| `KNW-POLE-007` | P+2 causal research is constrained to research-only and non-promotion in the recorded guardrails. | `KNOWN` | `A2`, `A10` |

### 3.8 ResearchDebt

| ROS ID | ResearchDebt | Why unresolved | Classification |
|---|---|---|---|
| `DEBT-POLE-001` | Concentration risk | Baseline is positive but BTC and BINANCE contribution concentration remains material. | `KNOWN` |
| `DEBT-POLE-002` | Need expanded MEXC/non-BTC sample or concentration controls | Explicit next-phase recommendation but no resolution recorded in inspected artifacts. | `KNOWN` |
| `DEBT-POLE-003` | P+2 exact outcome values | P+2 audit spec and comparison mechanism are present, but exact output metrics are not recorded in the inspected summary doc. | `UNKNOWN` |
| `DEBT-POLE-004` | Missing reproducibility command lines for every historical run | The snapshot records artifact paths and assumptions, but not exact commands for all runs. | `NEVER RECORDED` |
| `DEBT-POLE-005` | Missing export artifacts referenced by `A1` | The snapshot references export paths, but the files were not present in the inspected working tree. | `LOST` |

### 3.9 Promotion history

| ROS ID | Promotion event | Outcome | Classification |
|---|---|---|---|
| `PROM-POLE-001` | Promotion to validated positive research baseline | Accepted as `VALIDATED_POSITIVE_RESEARCH_BASELINE / CONCENTRATED_EDGE`. | `KNOWN` |
| `PROM-POLE-002` | Promotion to production | Explicitly not promoted and not production. | `KNOWN` |
| `PROM-POLE-003` | P+2 motif promotion | Explicitly forbidden by guardrails: do not promote this motif from this result. | `KNOWN` |

### 3.10 Current state

| Field | Current ROS state | Classification |
|---|---|---|
| Family state | Validated positive research baseline with concentration warning | `KNOWN` |
| Production state | Not production / not promoted | `KNOWN` |
| Baseline rollback state | Fixed Next Open baseline is the reconstructable research rollback point for this family | `KNOWN` |
| Main unresolved risk | Concentration risk | `KNOWN` |
| Recommended next work | Expand MEXC/non-BTC sample or add concentration controls | `KNOWN` |
| Whether historical family can be reconstructed from ROS representation alone | Mostly yes for high-level decision state; not fully for exact raw run replay because exports and exact commands are missing | `KNOWN` |

---

## 4. Knowledge graph

```text
OBS-POLE-001
  -> BEL-POLE-001
  -> HYP-POLE-001
  -> VR-POLE-001 / VR-POLE-002 / VR-POLE-003
  -> EVD-POLE-001..018
  -> DEC-POLE-001
  -> KNW-POLE-001 / KNW-POLE-002
  -> PROM-POLE-001

OBS-POLE-003 / OBS-POLE-004 / OBS-POLE-005
  -> BEL-POLE-005
  -> HYP-POLE-003
  -> VR-POLE-005 / VR-POLE-006 / VR-POLE-007
  -> EVD-POLE-015..018
  -> DEC-POLE-003 / DEC-POLE-006
  -> KNW-POLE-003 / KNW-POLE-004
  -> DEBT-POLE-001 / DEBT-POLE-002

OBS-POLE-007
  -> BEL-POLE-006
  -> HYP-POLE-004
  -> VR-POLE-008 / VR-POLE-009
  -> EVD-POLE-019 / EVD-POLE-020
  -> DEC-POLE-005
  -> KNW-POLE-006 / KNW-POLE-007
  -> DEBT-POLE-003
```

**Graph evaluation:** The ROS model can represent positive knowledge, negative knowledge, unresolved debt, promotion boundaries, and future observations without introducing detector or strategy concepts as roots.

---

## 5. Decision chronology

| Order | Date | Decision | Evidence / rationale | ROS object(s) | Classification |
|---:|---|---|---|---|---|
| 1 | `UNKNOWN` | Select Pole Core Motif as a research family | Repository contains multiple pole motif audit implementations; exact initial decision date not recorded. | `OBS-POLE-001`, `BEL-POLE-001` | `UNKNOWN` |
| 2 | `UNKNOWN` | Test `NEXT_COLUMN_OPEN_ENTRY` as observable entry | Next Open expectancy and robustness audit implementations exist; exact first run date not recorded. | `HYP-POLE-001`, `VR-POLE-001` | `UNKNOWN` |
| 3 | `UNKNOWN` | Accept edge as surviving execution reality | Research log records `EDGE_SURVIVES`. | `VR-POLE-002`, `DEC-POLE-001` | `KNOWN` result / `UNKNOWN` exact date |
| 4 | `UNKNOWN` | Accept execution model v1 as execution-ready | Research log records `EXECUTION_READY`. | `VR-POLE-003`, `DEC-POLE-001` | `KNOWN` result / `UNKNOWN` exact date |
| 5 | `UNKNOWN` | Add break-even after +2R to fixed baseline | Research log records `BE_IMPROVES`. | `VR-POLE-004`, `KNW-POLE-005` | `KNOWN` result / `UNKNOWN` exact date |
| 6 | `UNKNOWN` | Mark portfolio as fragile | Research log records `PORTFOLIO_FRAGILE` due to `SYMBOL_CONCENTRATION`. | `VR-POLE-005`, `DEC-POLE-003` | `KNOWN` result / `UNKNOWN` exact date |
| 7 | `UNKNOWN` | Mark edge as partially concentrated but not BTC-dependent | Research log records `EDGE_PARTIALLY_CONCENTRATED`, not BTC-dependent. | `VR-POLE-006`, `KNW-POLE-003` | `KNOWN` result / `UNKNOWN` exact date |
| 8 | 2026-06-03 | Freeze snapshot as `VALIDATED_POSITIVE_RESEARCH_BASELINE / CONCENTRATED_EDGE` | Research result log snapshot and metrics. | `PROM-POLE-001` | `KNOWN` |
| 9 | 2026-06-03 | Do not promote to production | Research log warnings state not production and not promoted. | `PROM-POLE-002`, `DEC-POLE-002` | `KNOWN` |
| 10 | 2026-06-03 | Define P+2 causal audit with no-promotion guardrail | P+2 doc states objective and guardrails. | `VR-POLE-008`, `DEC-POLE-005` | `KNOWN` |

---

## 6. Evidence inventory

### 6.1 Fixed research baseline metrics

| Metric | Value | Classification |
|---|---:|---|
| Trades | 460 | `KNOWN` |
| Wins | 332 | `KNOWN` |
| Losses | 69 | `KNOWN` |
| Break-even exits | 59 | `KNOWN` |
| Win rate | 72.17% | `KNOWN` |
| BE rate | 12.83% | `KNOWN` |
| Expectancy | +1.654R/trade | `KNOWN` |
| Total R | +761R | `KNOWN` |
| Max drawdown | 4R | `KNOWN` |
| Longest losing streak | 4 | `KNOWN` |
| Median concurrent positions | 1 | `KNOWN` |
| Max concurrent positions | 2 | `KNOWN` |
| Average active risk | ~1.03R | `KNOWN` |
| Peak active risk | 2R | `KNOWN` |

### 6.2 Market-family metrics

| Market family | Expectancy | Classification |
|---|---:|---|
| BINANCE | +1.655R | `KNOWN` |
| MEXC | +1.653R | `KNOWN` |

### 6.3 Concentration metrics

| Metric | Value | Classification |
|---|---:|---|
| BTC contribution | ~60.8% of total R | `KNOWN` |
| BINANCE contribution | ~81.5% of total R | `KNOWN` |
| Final stability verdict | `CONCENTRATED_EDGE` | `KNOWN` |

### 6.4 Causal comparison evidence

| Baseline / candidate | Trade count | Expectancy | Classification |
|---|---:|---:|---|
| Historical non-causal core motif | 460 | +1.654R/trade | `KNOWN` |
| Causal P+4 true-birth revalidation | `UNKNOWN` | -0.714R/trade | `KNOWN` expectancy / `UNKNOWN` trade count |
| Causal P+2 motif | `UNKNOWN` | `UNKNOWN` | `UNKNOWN` exact output values in inspected docs |

### 6.5 Artifact path evidence from research log

| Artifact path recorded in history | Status in ROS migration |
|---|---|
| `pnf_mvp/exports/pole_execution_model_v1_7markets_v1/execution_model_v1_summary.md` | Path preserved; file not present in inspected working tree: `LOST` |
| `pnf_mvp/exports/pole_be_research_7markets_v1/be_research_summary.md` | Path preserved; file not present in inspected working tree: `LOST` |
| `pnf_mvp/exports/pole_portfolio_reality_7markets_v1/portfolio_reality_summary.md` | Path preserved; file not present in inspected working tree: `LOST` |
| `pnf_mvp/exports/pole_symbol_dependence_7markets_v1/pole_symbol_dependence_summary.md` | Path preserved; file not present in inspected working tree: `LOST` |
| `pnf_mvp/exports/pole_baseline_stability_market_fix_7markets_v1/baseline_stability_market_fix_summary.md` | Path preserved; file not present in inspected working tree: `LOST` |

---

## 7. Research debt inventory

| Debt ID | Debt | Impact | Required resolution |
|---|---|---|---|
| `DEBT-POLE-001` | Concentration risk | Prevents treating the edge as fully diversified. | Expand MEXC/non-BTC sample or add concentration controls. |
| `DEBT-POLE-002` | Missing exact commands for all historical runs | Weakens reproducibility. | Future ROS registry should require command/config/hash fields for every ValidationRun. |
| `DEBT-POLE-003` | Missing referenced export artifacts | Prevents reconstruction of every intermediate table from repository state alone. | Preserve exported summaries/manifests or ingest them into ROS evidence records. |
| `DEBT-POLE-004` | P+2 exact output values absent from inspected summary | Limits decision reconstruction for causal P+2 follow-up. | Locate existing `p2_causal_motif_*` outputs or rerun only if explicitly authorized in a future research task. |
| `DEBT-POLE-005` | Initial observation / first hypothesis date not recorded | Weakens chronology before final snapshot. | Backfill dates from git history or original PRs if available. |

---

## 8. Promotion history

| Promotion stage | Status | Rationale | Classification |
|---|---|---|---|
| Candidate research family | Accepted for research | Multiple audit implementations and research log record family. | `KNOWN` |
| Validated positive research baseline | Promoted | Fixed baseline positive and survived execution-reality, break-even, portfolio, symbol-dependence, and market-family checks. | `KNOWN` |
| Fully diversified baseline | Not promoted | Contribution concentration remains material. | `KNOWN` |
| Production strategy | Not promoted | Research log explicitly says not production and not promoted. | `KNOWN` |
| P+2 causal motif | Not promoted | Guardrail says do not promote this motif from this result. | `KNOWN` |

---

## 9. Rollback history

| Rollback point | Definition | Classification | Rationale |
|---|---|---|---|
| `RB-POLE-001` | Fixed research baseline: `NEXT_COLUMN_OPEN_ENTRY`, fixed 3-box stop, fixed 2.5R target, break-even after +2R, no TP1/TP2/trailing/scaling/pyramiding/filters/optimization variants. | `KNOWN` | This is the fixed research baseline recorded in `A1`. |
| `RB-POLE-002` | Non-production guardrail | `KNOWN` | If later work attempts production activation, rollback to research-only status. |
| `RB-POLE-003` | Concentration warning | `KNOWN` | If later work claims diversified edge without new evidence, rollback to `CONCENTRATED_EDGE`. |
| `RB-POLE-004` | P+2 no-promotion guardrail | `KNOWN` | P+2 audit explicitly says not to promote this motif from the result. |

---

## 10. Current knowledge state

The current reconstructable knowledge state is:

1. The Pole Core Motif / Next Open Entry family is a **validated positive research baseline**, not production.
2. The fixed baseline is:
   - Entry: `NEXT_COLUMN_OPEN_ENTRY`.
   - Stop: fixed 3-box stop.
   - Target: fixed 2.5R.
   - Management: move stop to break-even after +2R.
   - No TP1, TP2, trailing, scaling, pyramiding, filters, or optimization variants.
3. The baseline metrics are 460 trades, 332 wins, 69 losses, 59 BE exits, 72.17% win rate, +1.654R/trade expectancy, and +761R total R.
4. The family is not production and was not promoted to live trading from the recorded research state.
5. The edge is positive in both BINANCE and MEXC market families.
6. The edge is materially concentrated: BTC contributes about 60.8% of total R and BINANCE contributes about 81.5% of total R.
7. The current stability verdict is `CONCENTRATED_EDGE`.
8. The next recommended research direction is to address concentration risk by expanding MEXC/non-BTC sample or adding concentration controls.
9. P+2 causal research exists as a follow-on investigation but is explicitly research-only and not promoted from the recorded result.
10. P+4 true-birth causal revalidation is a negative comparison baseline at -0.714R/trade.

---

## 11. Missing metadata

| Missing field | Classification | Why this classification |
|---|---|---|
| Exact first observation date | `UNKNOWN` | The inspected artifacts do not identify when the motif was first observed. |
| Exact first hypothesis author | `UNKNOWN` | The inspected artifacts do not record authorship. |
| Exact command lines for all validation runs | `NEVER RECORDED` | The high-level research log records assumptions and artifact paths, not all commands. |
| Git commit hash for each validation run | `NEVER RECORDED` | No per-run commit hash appears in inspected summaries. |
| Full exported intermediate CSV/JSON/summary contents for `A1` artifact paths | `LOST` | The paths are recorded, but the files were not present in the inspected working tree. |
| P+2 exact trades / wins / losses / BE exits / expectancy / total R | `UNKNOWN` | The P+2 doc defines required outputs and comparison, but the inspected doc does not record the resulting values. |
| P+4 trade count | `UNKNOWN` | The comparison records expectancy but not trade count. |
| Independent review sign-off | `NEVER RECORDED` | No review approval field appears in inspected artifacts. |
| Dataset content snapshots | `NOT APPLICABLE` | This documentation-only migration does not inspect or create datasets. |
| Detector version changes | `NOT APPLICABLE` | This migration is not detector development and records no detector changes. |
| Strategy code changes | `NOT APPLICABLE` | This migration is not strategy development and records no strategy changes. |

---

## 12. Architecture evaluation

### Did any historical information fail to fit the ROS?

**No.** The historical information that exists in the repository fits the ROS model.

The family maps cleanly as:

```text
Observation
  -> Belief
  -> Hypothesis
  -> ValidationRun
  -> Evidence
  -> Decision
  -> Knowledge
  -> ResearchDebt / Promotion / Rollback
  -> New Observation
```

### Why it fits

| Historical need | ROS representation |
|---|---|
| Market structure was noticed | Observation |
| Research interpretation formed | Belief |
| Testable claim about Next Open entry and fixed risk model | Hypothesis |
| Audits executed | ValidationRun |
| Metrics and verdicts produced | Evidence |
| Research-only / no-promotion / concentration decisions made | Decision |
| Fixed baseline and warnings preserved | Knowledge |
| Concentration and missing causal outputs remain unresolved | ResearchDebt |
| Research baseline accepted, production promotion rejected | Promotion history |
| Fixed baseline and guardrails preserved | Rollback history |

### What did not fit?

Nothing required a new root ROS object.

The items that were incomplete were not architectural failures. They were missing historical metadata or missing export artifacts:

- Exact commands for every run.
- Per-run git commit hashes.
- Missing exported summary files referenced by the research log.
- P+2 exact output metrics.
- Initial observation date and author.

These are evidence/provenance completeness problems, not ROS concept problems.

---

## 13. ROS deficiencies

### Deficiencies found in ROS concepts

**None for this family.**

The ROS root concepts were sufficient to represent:

- Positive evidence.
- Negative evidence.
- Research-only decisions.
- Non-promotion decisions.
- Concentration warnings.
- Rollback points.
- Missing metadata.
- Open research debt.
- Future recommendations.

### Deficiencies found in current historical records

| Deficiency | ROS object affected | Severity | Notes |
|---|---|---|---|
| Missing per-run commands | ValidationRun | High | Reproducibility is weakened without commands. |
| Missing export artifacts | Evidence | High | High-level metrics survive, but intermediate evidence cannot be reconstructed. |
| Missing exact dates for pre-snapshot steps | Decision chronology | Medium | Final 2026-06-03 snapshot survives, but earlier chronology is incomplete. |
| Missing causal P+2 exact values in inspected docs | Evidence / Decision | Medium | P+2 audit is defined but exact outputs are not in the inspected summary. |
| Missing review sign-off | Promotion | Medium | Promotion to research baseline is recorded, but reviewer identity/sign-off is not. |

---

## 14. Recommendations

1. **Begin ROS-0 migration with this family as the seed registry record.**
   - It contains enough complete positive and negative history to test the model.

2. **Create minimal registries, not new architecture layers.**
   - `observations`
   - `beliefs`
   - `hypotheses`
   - `validation_runs`
   - `evidence`
   - `decisions`
   - `knowledge`
   - `research_debt`
   - `promotions`
   - `rollback_points`

3. **Require future ValidationRun records to include reproducibility metadata.**
   - Command.
   - Config path.
   - Input artifact paths.
   - Output artifact paths.
   - Git commit hash.
   - Universe.
   - Metric schema.
   - Promotion eligibility flag.

4. **Ingest referenced historical export summaries if they still exist outside this working tree.**
   - If they cannot be recovered, keep their status as `LOST` and do not fabricate them.

5. **Preserve the fixed research baseline exactly.**
   - Do not alter entry, stop, target, BE logic, or concentration status as part of ROS migration.

6. **Record the concentration warning as first-class knowledge and research debt.**
   - It is central to the current state and prevents accidental production interpretation.

7. **Do not promote P+2 causal motif from this migration.**
   - Its own guardrails explicitly forbid promotion from that result.

8. **Use this migration to define the minimum completeness standard for future ROS records.**
   - A future completed family should not rely on missing exports or unrecorded command lines.

---

## 15. Final verdict

**ROS_PROVEN_FOR_THIS_FAMILY**

Precise justification:

1. The ROS model represented the complete known lifecycle for the Pole Core Motif / Next Open Entry family without requiring a new root concept.
2. Observations, beliefs, hypotheses, validation runs, evidence, decisions, knowledge, research debt, promotion history, rollback history, current state, and future recommendations all fit the model.
3. The model preserved the critical distinction between validated research baseline and production promotion.
4. The model preserved negative and cautionary knowledge, especially concentration risk and causal-audit non-promotion.
5. The only reconstruction gaps came from missing historical metadata and missing referenced export files, not from ROS object insufficiency.

Independent reconstruction assessment:

- An independent engineer could reconstruct the **decision state, fixed baseline, core evidence, warnings, promotion boundaries, rollback points, and open questions** from this ROS representation alone.
- An independent engineer could **not** reconstruct every intermediate exported table or exact historical command line from this ROS representation alone because those details are absent or lost in the inspected repository artifacts.
- Therefore ROS is proven conceptually sufficient for this family, while the historical record reveals stricter evidence-retention requirements for future ROS-0 registries.
