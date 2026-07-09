# ROS Pre-Flight Audit

## Scope

This is a repository documentation and knowledge inventory performed before any ROS migration begins. It is an audit only: no detector work, implementation, research, validation, migration, dataset creation, backtesting, optimization, or existing-document rewrite was performed.

Inventory source: committed repository files matching `*.md`, `*.txt`, `*.json`, and `*.csv` from `git ls-files`.

## Executive Summary

The repository contains a substantial historical research record, but knowledge is fragmented across root-level ROS documents, legacy `docs/`, `experiments/`, `pnf_mvp/exports/`, and `research_v2/` artifacts. The highest-value migration work is to preserve the profitable Pole baseline, final validation outputs, acceptance rules, research logs, continuation-failure closure/synthesis, and the harmonic/ABCD research chains before attempting any new detector work.

No migration should start by creating new research. ROS-0 should first convert authoritative documents into inventory records, identify successor/superseded chains, and attach each serious report to a hypothesis, validation run, failure record, promotion decision, or research debt item.

## Repository Statistics

| Metric | Count | Notes |
| --- | ---: | --- |
| Total committed documentation/artifact files audited | 142 | Markdown, text, JSON, and CSV files. |
| Markdown documents | 58 | Primary human-readable knowledge. |
| Text documents | 1 | BE experiment package note. |
| JSON documents/config manifests | 5 | Settings and portfolio/live config examples. |
| CSV artifact tables | 78 | Validation, research, and exported result tables. |
| Architecture / design docs | 15 | Includes platform, ROS, research_v2 scaffolding, structure, harmonic, and ABCD designs. |
| Research / experiment docs and artifacts | 79 | Includes Pole, continuation, harmonic, ABCD, structural, ratio, and pattern outputs. |
| Validation docs and artifacts | 31 | Includes final validation, pole summaries, BTC follow-through, and portfolio reality outputs. |
| Execution / live docs | 5 | MEXC, settings, replay, and trade snapshot docs. |
| Governance docs | 3 | AGENTS, workflow, acceptance rules. |
| Failure / closure reports | 2 | Continuation-failure closure and synthesis. |
| ROS / migration docs | 4 | Blueprint, Advanced, Decision Engine, Knowledge Migration Review. |

## Inventory Categories

### Architecture / Design

| Document | Purpose | Status | Owner | Family | Dependencies | References | Referenced by | Active? | Authority |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `RESEARCH_OPERATING_SYSTEM_BLUEPRINT.md` | Core ROS architecture and contracts. | Current architecture | Unknown | ROS / Migration | Existing project knowledge | None detected | `RESEARCH_OPERATING_SYSTEM_ADVANCED.md`, `ROS_KNOWLEDGE_MIGRATION_REVIEW.md` | Active | High |
| `RESEARCH_OPERATING_SYSTEM_ADVANCED.md` | Advanced ROS knowledge graph, confidence, debt, composition. | Current extension | Unknown | ROS / Migration | Blueprint | Blueprint | `ROS_KNOWLEDGE_MIGRATION_REVIEW.md` | Active | High |
| `RESEARCH_DECISION_ENGINE.md` | Prioritization architecture for next research actions. | Current extension | Unknown | ROS / Migration | Blueprint, Advanced, Migration Review | None detected by filename | None detected | Active | High |
| `ROS_KNOWLEDGE_MIGRATION_REVIEW.md` | Maps historical knowledge into ROS. | Current migration review | Unknown | ROS / Migration | Blueprint, Advanced | Blueprint, Advanced | None detected | Active | High |
| `README_project_workflow.md` | Project research-lab workflow and operating model. | Current governance/design | Unknown | Strategy Validation / Platform | Repository pipeline | None detected | None detected | Active | High |
| `docs/PNF_SAAS_PLATFORM_ARCHITECTURE.md` | Future SaaS platform planning. | Planning only | Unknown | Platform | Existing scanner/trader/research tooling | None detected | `docs/PNF_PLATFORM_CAPABILITY_AUDIT.md` | Active for product planning | Medium |
| `docs/PNF_PLATFORM_CAPABILITY_AUDIT.md` | Audit of computed vs displayed platform capability. | Planning audit | Unknown | Platform / UI | Platform architecture, app files | SaaS architecture | None detected | Active for UI roadmap | Medium |
| `docs/incremental_structure_engine_design.md` | Incremental structure engine design. | Planning only | Unknown | Structure Engine | Existing structure engine | None detected | None detected | Active/historical | Medium |
| `research_v2/README.md` | Research v2 isolated path rationale. | Scaffolding | Unknown | Research Infrastructure | Existing mixed workflow | None detected | None detected | Active | High |
| `research_v2/structure_validation/pnf_structural_swing_aggregation_design.md` | Causal structural swing aggregation design. | Research-only design | Unknown | Structure Engine / Harmonic | PnF columns, structural swings | None detected | None detected | Active | High |
| `research_v2/patterns/pnf_harmonic_swing_framework_design.md` | PnF-native harmonic swing framework design. | Phase 0 design | Unknown | Harmonic | Structural swing extraction | None detected | None detected | Active/historical | High |
| `research_v2/patterns/pnf_harmonic_swing_extraction_design.md` | Harmonic-grade swing extraction design. | Phase 0 design | Unknown | Harmonic | Raw completed PnF columns | None detected | None detected | Active/historical | High |
| `research_v2/patterns/pnf_harmonic_swing_model_comparison.md` | Compare swing definitions for harmonic geometry. | Phase 0 design study | Unknown | Harmonic / Structure | Harmonic framework/extraction concepts | None detected | None detected | Active/historical | High |
| `research_v2/patterns/pnf_abcd_symmetry_audit_design.md` | Initial AB=CD symmetry audit design. | Superseded by v2 | Unknown | ABCD | Validated harmonic artifacts | Harmonic artifacts by text | None detected | Historical | Medium |
| `research_v2/patterns/pnf_abcd_symmetry_audit_design_v2.md` | Revised AB=CD audit design closing gaps. | Successor design | Unknown | ABCD | Initial AB=CD design, harmonic baseline | None detected | ABCD outcome reports | Active | High |
| `research_v2/patterns/pole_genetic_hypothesis_miner_design.md` | Causal Pole genetic hypothesis miner design. | Phase 0 design only | Unknown | Pole | Pole setups | None detected | `research_v2/patterns/pole_genetic_hypothesis_miner_review.md` | Historical/active candidate | Medium |
| `research_v2/patterns/pole_genetic_hypothesis_miner_review.md` | Review of Pole genetic miner design. | Research-only review | Unknown | Pole | Miner design | Miner design | None detected | Historical | Medium |

### Governance / Workflow / Strategy Rules

| Document | Purpose | Status | Owner | Family | Dependencies | References | Referenced by | Active? | Authority |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `AGENTS.md` | Repository governance, baseline rollback, protected interfaces, scorecard. | Current governance | Unknown | Strategy / Baseline | Current profitable baseline | None detected | Prompt/context | Active | Very High |
| `experiments/acceptance_rules.md` | Experiment acceptance rules and required scorecard. | Current experiment governance | Unknown | Strategy Validation | Stable baseline | None detected | None detected | Active | High |
| `README_project_workflow.md` | Research-lab workflow and pipeline intent. | Current workflow | Unknown | Strategy Validation | Collector/DB/PnF/structure/strategy pipeline | None detected | None detected | Active | High |

### Research / Validation / Reports

| Document | Purpose | Status | Owner | Family | Dependencies | References | Referenced by | Active? | Authority |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `docs/research_results_log.md` | Versioned strategy/validation research snapshots. | Active log | Unknown | Pole / Strategy Validation | Settings, validation outputs | `pnf_mvp/settings.research_clean.json`, portfolio reality report path | None detected | Active | Very High |
| `docs/pnf_research_checkpoint.md` | Research checkpoint and technical state. | Checkpoint | Unknown | Strategy Validation / Structure | Diagnostics/export fields, validation/backfills | None detected | None detected | Historical snapshot | Medium |
| `docs/p2_causal_motif_research.md` | Full 7-market P+2 causal pole/reversal motif audit. | Research report | Unknown | Pole / SL-C / Causal Motif | 7-market motif audit data | None detected | None detected | Historical | High |
| `experiments/experiment_log.md` | Experiment log and stable baseline reference. | Active/historical log | Unknown | Pole / Baseline | Stable baseline metrics | `experiments/structural_pattern_outcome_summary_v1.md` | None detected | Active | Very High |
| `experiments/structural_pattern_outcome_summary_v1.md` | Unified structural pattern outcome summary. | Research report | Unknown | Structure / Pattern Outcomes | Shadow scanner exports | Scanner/export paths by text | `experiments/experiment_log.md` | Historical | High |
| `experiments/btc_followthrough_investigation.md` | BTC follow-through investigation with no logic redesign. | Investigation report | Unknown | Pole / BTC / Follow-through | Temporary BTC validation artifacts | None detected | None detected | Historical | Medium |
| `experiments/btc_pole_followthrough_validation.md` | BTC follow-through validation rerun. | Validation report | Unknown | Pole / BTC / Forward-like validation | Data-contract fix rerun | None detected | None detected | Historical | Medium |
| `pnf_mvp/exports/entry_distance_decay_v1.md` | Entry distance decay analysis. | Research report | Unknown | Next Open Entry / Entry Distance | Companion CSV | `pnf_mvp/exports/entry_distance_decay_v1.csv` by companion | None detected | Historical | Medium |
| `pnf_mvp/exports/research_closure_continuation_failure/research_closure.md` | Continuation failure closure report. | Closure / failure report | Unknown | Continuation Failure | Continuation research artifacts | None detected | None detected | Historical, high migration value | High |
| `pnf_mvp/exports/research_synthesis_continuation_failure/research_synthesis.md` | Continuation failure synthesis. | Synthesis / failure report | Unknown | Continuation Failure | Closure and continuation findings | None detected | None detected | Historical, high migration value | High |
| `pnf_mvp/exports/final_validation_pr_20260701/portfolio_reality_from_raw/final_validation_report.md` | Final validation report. | Validation report | Unknown | Pole / Portfolio Reality | Research clean settings, portfolio reality artifacts | `pnf_mvp/settings.research_clean.json` | `docs/research_results_log.md` indirectly by path family | Active baseline evidence | Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/portfolio_reality_from_raw/portfolio_reality_summary.md` | Portfolio reality summary. | Validation summary | Unknown | Pole / Portfolio Reality | Portfolio reality CSVs/manifest | None detected | `docs/research_results_log.md` | Active baseline evidence | Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/poles/BNB/pole_summary.md` | BNB pole validation summary. | Symbol validation summary | Unknown | Pole | BNB columns/pole patterns | Companion CSVs | None detected | Historical/baseline support | High |
| `pnf_mvp/exports/final_validation_pr_20260701/poles/BTC/pole_summary.md` | BTC pole validation summary. | Symbol validation summary | Unknown | Pole | BTC columns/pole patterns | Companion CSVs | None detected | Historical/baseline support | High |
| `pnf_mvp/exports/final_validation_pr_20260701/poles/ETH/pole_summary.md` | ETH pole validation summary. | Symbol validation summary | Unknown | Pole | ETH columns/pole patterns | Companion CSVs | None detected | Historical/baseline support | High |
| `pnf_mvp/exports/final_validation_pr_20260701/poles/SOL/pole_summary.md` | SOL pole validation summary. | Symbol validation summary | Unknown | Pole | SOL columns/pole patterns | Companion CSVs | None detected | Historical/baseline support | High |
| `pnf_mvp/exports/final_validation_pr_20260701/poles/XRP/pole_summary.md` | XRP pole validation summary. | Symbol validation summary | Unknown | Pole | XRP columns/pole patterns | Companion CSVs | None detected | Historical/baseline support | High |
| `research_v2/patterns/ABCD_MODEL_C_RESEARCH_SUMMARY.md` | AB=CD Model C research summary. | Stable reference summary | Unknown | ABCD | AB=CD Model C work | None detected | None detected | Active/historical | High |
| `research_v2/patterns/abcd_geometry_input_diagnostic.md` | AB=CD geometry input diagnostic. | Infrastructure diagnostic | Unknown | ABCD | Harmonic reactions artifacts | Harmonic reactions CSVs | None detected | Historical | Medium |
| `research_v2/patterns/abcd_population_local_v1/abcd_population_report.md` | AB/CD population audit report. | BLOCKED | Unknown | ABCD | Missing trusted harmonic input | Harmonic reaction CSV paths | None detected | Historical blocked | Medium |
| `research_v2/patterns/abcd_outcome_local_v1/abcd_outcome_report.md` | AB=CD structural outcome audit report. | BLOCKED | Unknown | ABCD | Missing Phase 3-approved input | AB=CD v2 design, harmonic reaction CSV paths | Sanity audit references outcome report | Historical blocked | Medium |
| `research_v2/patterns/abcd_outcome_distance_local_v1/abcd_outcome_distance_report.md` | AB=CD outcome distance audit. | Research audit | Unknown | ABCD | Geometry candidates, harmonic next pivots | Harmonic reaction CSV paths | None detected | Historical | Medium |
| `research_v2/patterns/abcd_outcome_repaired_local_v1/abcd_outcome_repaired_report.md` | Repaired AB=CD structural outcome audit. | BLOCKED | Unknown | ABCD | Missing Phase 3-approved input | AB=CD v2 design, harmonic reaction CSV paths | None detected | Historical blocked | Medium |
| `research_v2/patterns/abcd_outcome_sanity_audit_local_v1/outcome_direction_flow_report.md` | AB=CD outcome direction sanity audit. | Research-only sanity audit | Unknown | ABCD | Outcome report, validation/trace CSVs | Outcome report, companion CSVs | None detected | Historical | Medium |
| `research_v2/patterns/harmonic_swing_threshold_local_v2/ratio_predictive_report.md` | Harmonic ratio predictive report. | Research report | Unknown | Harmonic | Local v2 harmonic reactions | Harmonic reaction CSVs | None detected | Historical | High |
| `research_v2/patterns/harmonic_swing_threshold_local_v2/ratio_predictive_stability_report.md` | Harmonic ratio stability report. | Research report | Unknown | Harmonic | Ratio predictive outputs | None detected | None detected | Historical | High |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/raw_ratio_density_report.md` | Raw harmonic ratio density report. | Research audit | Unknown | Harmonic | Audit reaction ratios | None detected | None detected | Historical | High |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/harmonic_time_stability_report.md` | Harmonic time stability report. | Research audit | Unknown | Harmonic | Harmonic audit tables | None detected | None detected | Historical | High |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/ratio_predictive_report.md` | BTC/ETH/SOL harmonic ratio predictive report. | Research report | Unknown | Harmonic | Audit harmonic reactions | Harmonic reaction CSVs | None detected | Historical | High |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/ratio_predictive_stability_report.md` | BTC/ETH/SOL harmonic ratio stability report. | Research report | Unknown | Harmonic | Ratio predictive audit outputs | None detected | None detected | Historical | High |

### Execution / Live Trading / Config

| Document | Purpose | Status | Owner | Family | Dependencies | References | Referenced by | Active? | Authority |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `pnf_mvp/README.md` | Real-time standalone PnF MVP README. | MVP docs | Unknown | Execution / Platform | Binance public API, SQLite | `settings.json`, positive expectancy settings | None detected | Historical/operational | Medium |
| `pnf_mvp/TRADE_SNAPSHOT_INTEGRATION.md` | Trade snapshot integration design. | Integration design | Unknown | Execution / Validation Store | Validation store fields | None detected | None detected | Active/historical | Medium |
| `pnf_mvp/README_BE_EXPERIMENT.txt` | Breakeven experiment package note. | Experimental package note | Unknown | Trade Management / BE | BE module files | None detected | None detected | Historical/experimental | Low-Medium |
| `research_v2/execution/README.md` | Execution simulation placeholder and MEXC config note. | Future work / docs | Unknown | Execution / MEXC | MEXC config example | `mexc_pole_live_config.example.json` | None detected | Active | High |
| `mexc_pole_live_config.example.json` | Safe-default MEXC Pole live config example. | Example config | Unknown | MEXC / Live Trading | MEXC live runner expectations | None | `research_v2/execution/README.md` | Active | High |
| `replay_summary.md` | MEXC Pole live replay parity summary. | Replay/parity summary | Unknown | MEXC / Live Replay | Replay run | None detected | None detected | Historical | Medium |
| `pnf_mvp/settings.json` | MVP runtime settings. | Config | Unknown | Execution | MVP README | None | `pnf_mvp/README.md` | Active/historical | Medium |
| `pnf_mvp/settings.research_clean.json` | Research-clean settings. | Config / validation dependency | Unknown | Validation / Baseline | Validation reports | None | Research log, final validation report, setup/labeling READMEs | Active baseline dependency | High |
| `pnf_mvp/settings.binance_demo_positive_expectancy.json` | Binance demo positive-expectancy settings. | Demo config | Unknown | Execution / Demo | MVP README | None | `pnf_mvp/README.md` | Historical | Medium |

### Research Infrastructure READMEs

| Document | Purpose | Status | Owner | Family | Dependencies | References | Referenced by | Active? | Authority |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `data/research/README.md` | Research data root instructions. | Active data guidance | Unknown | Research Infrastructure | research_v2 artifacts | None | None detected | Active | High |
| `research_v2/setup_dataset/README.md` | Frozen setup dataset export path. | Active infra docs | Unknown | Dataset / Setup Export | Research clean settings | `pnf_mvp/settings.research_clean.json` | None detected | Active | High |
| `research_v2/labeling/README.md` | Deterministic research labeling engine. | Active infra docs | Unknown | Labeling | Setup datasets, settings | `pnf_mvp/settings.research_clean.json` | None detected | Active | High |
| `research_v2/analytics/README.md` | Outcome-focused analytics docs. | Active infra docs | Unknown | Analytics | DuckDB optional dependency | None detected | None detected | Active | High |
| `research_v2/filters/README.md` | Research-only filter stage. | Active infra docs | Unknown | Filters / Baseline | Profitable family v1 profile | None detected | None detected | Active | High |

### CSV / JSON Artifact Inventory

The following machine-readable artifacts are committed documentation/research artifacts. Owners are not declared in-file unless otherwise noted. Most have no direct outgoing references because they are data/config artifacts; incoming references are generally from their companion reports or READMEs.

| Artifact | Category | Purpose / family | Status | Key dependency or companion | Active vs historical | Authority |
| --- | --- | --- | --- | --- | --- | --- |
| `pnf_mvp/exports/entry_distance_decay_v1.csv` | Experiment artifact | Entry distance decay table / Next Open Entry | Companion to entry-distance report | `entry_distance_decay_v1.md` | Historical | Medium |
| `pnf_mvp/exports/final_validation_pr_20260701/columns/BNB_columns.csv` | Validation artifact | BNB PnF columns | Final validation input/output | BNB pole summary | Historical baseline support | High |
| `pnf_mvp/exports/final_validation_pr_20260701/columns/BTC_columns.csv` | Validation artifact | BTC PnF columns | Final validation input/output | BTC pole summary | Historical baseline support | High |
| `pnf_mvp/exports/final_validation_pr_20260701/columns/ETH_columns.csv` | Validation artifact | ETH PnF columns | Final validation input/output | ETH pole summary | Historical baseline support | High |
| `pnf_mvp/exports/final_validation_pr_20260701/columns/SOL_columns.csv` | Validation artifact | SOL PnF columns | Final validation input/output | SOL pole summary | Historical baseline support | High |
| `pnf_mvp/exports/final_validation_pr_20260701/columns/XRP_columns.csv` | Validation artifact | XRP PnF columns | Final validation input/output | XRP pole summary | Historical baseline support | High |
| `pnf_mvp/exports/final_validation_pr_20260701/poles/BNB/pole_patterns.csv` | Validation artifact | BNB pole patterns | Symbol output | BNB pole summary | Historical baseline support | High |
| `pnf_mvp/exports/final_validation_pr_20260701/poles/BTC/pole_patterns.csv` | Validation artifact | BTC pole patterns | Symbol output | BTC pole summary | Historical baseline support | High |
| `pnf_mvp/exports/final_validation_pr_20260701/poles/ETH/pole_patterns.csv` | Validation artifact | ETH pole patterns | Symbol output | ETH pole summary | Historical baseline support | High |
| `pnf_mvp/exports/final_validation_pr_20260701/poles/SOL/pole_patterns.csv` | Validation artifact | SOL pole patterns | Symbol output | SOL pole summary | Historical baseline support | High |
| `pnf_mvp/exports/final_validation_pr_20260701/poles/XRP/pole_patterns.csv` | Validation artifact | XRP pole patterns | Symbol output | XRP pole summary | Historical baseline support | High |
| `pnf_mvp/exports/final_validation_pr_20260701/portfolio_reality_from_raw/equity_curve_usdt.csv` | Validation artifact | Portfolio equity curve | Final validation output | Portfolio reality reports | Historical baseline support | High |
| `pnf_mvp/exports/final_validation_pr_20260701/portfolio_reality_from_raw/monthly_returns_usdt.csv` | Validation artifact | Monthly returns table | Final validation output | Portfolio reality reports | Historical baseline support | High |
| `pnf_mvp/exports/final_validation_pr_20260701/portfolio_reality_from_raw/portfolio_reality_equity_curve.csv` | Validation artifact | Portfolio reality equity curve | Final validation output | Portfolio reality reports | Historical baseline support | High |
| `pnf_mvp/exports/final_validation_pr_20260701/portfolio_reality_from_raw/portfolio_reality_flags.csv` | Validation artifact | Portfolio flags | Final validation output | Portfolio reality reports | Historical baseline support | High |
| `pnf_mvp/exports/final_validation_pr_20260701/portfolio_reality_from_raw/portfolio_reality_manifest.json` | Validation manifest | Portfolio reality manifest | Final validation manifest | Portfolio reality reports | Historical baseline support | Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/portfolio_reality_from_raw/portfolio_reality_monthly.csv` | Validation artifact | Portfolio monthly table | Final validation output | Portfolio reality reports | Historical baseline support | High |
| `pnf_mvp/exports/final_validation_pr_20260701/portfolio_reality_from_raw/portfolio_reality_quarterly.csv` | Validation artifact | Portfolio quarterly table | Final validation output | Portfolio reality reports | Historical baseline support | High |
| `pnf_mvp/exports/final_validation_pr_20260701/portfolio_reality_from_raw/portfolio_reality_symbol_contribution.csv` | Validation artifact | Symbol contribution table | Final validation output | Portfolio reality reports | Historical baseline support | High |
| `pnf_mvp/exports/final_validation_pr_20260701/portfolio_reality_from_raw/portfolio_reality_trade_sequence.csv` | Validation artifact | Trade sequence table | Final validation output | Portfolio reality reports | Historical baseline support | High |
| `pnf_mvp/strategy_diagnostics_breakdowns.csv` | Strategy artifact | Diagnostics breakdowns | Exported strategy review | Strategy review CSVs | Historical | Medium |
| `pnf_mvp/strategy_stopped_review.csv` | Strategy artifact | Stopped trade review | Exported strategy review | Strategy review CSVs | Historical | Medium |
| `pnf_mvp/strategy_tp2_review.csv` | Strategy artifact | TP2 review | Exported strategy review | Strategy review CSVs | Historical | Medium |
| `pnf_mvp/strategy_trades_export.csv` | Strategy artifact | Trade export | Exported strategy review | Strategy review CSVs | Historical | Medium |
| `research_v2/patterns/abcd_population_local_v1/abcd_population_summary.csv` | ABCD artifact | Population summary | Companion table | Population report | Historical blocked | Medium |
| `research_v2/patterns/abcd_population_local_v1/abcd_population_by_symbol.csv` | ABCD artifact | Population by symbol | Companion table | Population report | Historical blocked | Medium |
| `research_v2/patterns/abcd_population_local_v1/abcd_population_by_year.csv` | ABCD artifact | Population by year | Companion table | Population report | Historical blocked | Medium |
| `research_v2/patterns/abcd_outcome_local_v1/abcd_outcome_summary.csv` | ABCD artifact | Outcome summary | Companion table | Outcome report | Historical blocked | Medium |
| `research_v2/patterns/abcd_outcome_local_v1/abcd_outcome_by_symbol.csv` | ABCD artifact | Outcome by symbol | Companion table | Outcome report | Historical blocked | Medium |
| `research_v2/patterns/abcd_outcome_local_v1/abcd_outcome_by_year.csv` | ABCD artifact | Outcome by year | Companion table | Outcome report | Historical blocked | Medium |
| `research_v2/patterns/abcd_outcome_distance_local_v1/abcd_outcome_distance_summary.csv` | ABCD artifact | Outcome distance summary | Companion table | Distance report | Historical | Medium |
| `research_v2/patterns/abcd_outcome_distance_local_v1/abcd_outcome_distance_by_symbol.csv` | ABCD artifact | Outcome distance by symbol | Companion table | Distance report | Historical | Medium |
| `research_v2/patterns/abcd_outcome_distance_local_v1/abcd_outcome_distance_by_year.csv` | ABCD artifact | Outcome distance by year | Companion table | Distance report | Historical | Medium |
| `research_v2/patterns/abcd_outcome_distance_local_v1/abcd_outcome_distance_sample.csv` | ABCD artifact | Outcome distance sample | Companion table | Distance report | Historical | Medium |
| `research_v2/patterns/abcd_outcome_repaired_local_v1/abcd_outcome_repaired_summary.csv` | ABCD artifact | Repaired outcome summary | Companion table | Repaired report | Historical blocked | Medium |
| `research_v2/patterns/abcd_outcome_repaired_local_v1/abcd_outcome_repaired_by_symbol.csv` | ABCD artifact | Repaired outcome by symbol | Companion table | Repaired report | Historical blocked | Medium |
| `research_v2/patterns/abcd_outcome_repaired_local_v1/abcd_outcome_repaired_by_year.csv` | ABCD artifact | Repaired outcome by year | Companion table | Repaired report | Historical blocked | Medium |
| `research_v2/patterns/abcd_outcome_sanity_audit_local_v1/outcome_classification_validation.csv` | ABCD artifact | Outcome classification validation | Companion table | Sanity report | Historical | Medium |
| `research_v2/patterns/abcd_outcome_sanity_audit_local_v1/outcome_direction_trace_sample.csv` | ABCD artifact | Outcome direction trace sample | Companion table | Sanity report | Historical | Medium |
| `research_v2/patterns/harmonic_swing_threshold_local_v2/*.csv` | Harmonic artifact bundle | Local v2 harmonic thresholds, reactions, ratio predictive summaries, symbol/year splits | Companion bundle | Local v2 ratio reports | Historical | High |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/*.csv` | Harmonic artifact bundle | BTC/ETH/SOL harmonic audit outputs, ratio distributions, stability, survival, rankings, predictive splits | Companion bundle | Audit reports | Historical | High |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/columns/*.csv` | Harmonic input bundle | BTC/ETH/SOL PnF columns for harmonic audit | Input columns | Harmonic audit reports | Historical | High |
| `pnf_mvp/settings.json` | Config | MVP settings | Runtime config | MVP README | Historical/active | Medium |
| `pnf_mvp/settings.research_clean.json` | Config | Research-clean validation settings | Validation config | Research log, validation reports | Active baseline dependency | High |
| `pnf_mvp/settings.binance_demo_positive_expectancy.json` | Config | Demo settings | Demo config | MVP README | Historical | Medium |
| `mexc_pole_live_config.example.json` | Config | Safe MEXC live config example | Example config | Execution README | Active | High |

## Research Families Represented

| Family | Maturity | Active documents | Validation status | Implementation status | Promotion status | Rollback point | Successor docs | Obsolete / lower-authority docs |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Pole | Most mature | AGENTS, experiment log, research results log, final validation reports, pole summaries | Strongest available validation trail | Existing detector/workflow present | Stable baseline / protected | Current profitable Pole baseline | ROS docs, final validation, acceptance rules | Older BTC-only follow-through reports are historical |
| SL-C / P+2 causal motif | Research-stage | `docs/p2_causal_motif_research.md` | Audit report exists | No current promotion evidence found | Not promoted | Pole baseline | Needs ROS hypothesis/failure migration | None clearly detected |
| Next Open Entry / entry distance | Research-stage | `entry_distance_decay_v1.md/csv` | Report artifact exists | No implementation promotion found | Not promoted | Pole baseline | Needs hypothesis/feature mapping | None clearly detected |
| Continuation Failure | Closure/synthesis-stage | Continuation closure and synthesis reports | Follow-up closure exists | Not a detector implementation in this audit | Likely failure/negative knowledge | Pole baseline | Failure Registry migration should be successor | None clearly detected |
| Breakout / Failed Breakout | Embedded research theme | Pole docs, baseline filters, structure reports | Present through Pole validation context | Existing Pole logic context | Baseline-protected only | Pole baseline | ROS migration should extract as features/hypotheses | Not separately authoritative yet |
| ABCD | Research/design-stage with blocked reports | AB=CD v2 design, Model C summary, population/outcome reports | Several reports; multiple blocked by missing trusted inputs | No production implementation found | Not promoted | Pole baseline | v2 design supersedes initial design | Initial AB=CD design partially superseded |
| Harmonic | Research infrastructure/audit-stage | Harmonic framework/extraction/model comparison, local v2/audit reports | Multiple descriptive audits and stability reports | No strategy/detector promotion found | Not promoted | Pole baseline | Audit reports and model comparison | None clearly obsolete, but local v2 may be superseded by audit bundle for some questions |
| Execution / MEXC / Live Trading | Operational-doc stage | execution README, MEXC config, replay summary, MVP README | Replay parity summary exists | MEXC config example and MVP docs exist | Live eligibility not established by audit | Pole baseline | ROS runbook gap should be successor | Demo configs historical |
| Structure Engine | Design/research-stage | incremental structure design, structural swing aggregation design, structural pattern summary | Structural outcome summary exists | Existing structure engine referenced | Not a promoted edge | Pole baseline | ROS Feature Store migration | Incremental design may be historical planning |
| Strategy Validation | Mature but fragmented | acceptance rules, research log, final validation reports, strategy CSVs | Multiple validation artifacts | Existing validation store/workflow present | Supports baseline | Pole baseline | ROS validation manifests | Some raw strategy CSVs lack explicit report context |
| Forward Validation | Partial | BTC follow-through validation, live/forward concepts in ROS | Limited explicit forward docs found | Not independently implemented in docs | Not separately promoted | Pole baseline | ROS confidence history | BTC-only follow-through historical |

## Cross-Reference Analysis

### Duplicate or Near-Duplicate Documents

| Cluster | Documents | Assessment | Recommendation |
| --- | --- | --- | --- |
| ROS architecture set | Blueprint, Advanced, Decision Engine, Migration Review | Complementary, not duplicate | Keep all; migrate as ROS architecture seed docs. |
| Harmonic ratio reports | Local v2 and BTC/ETH/SOL audit ratio predictive/stability reports | Similar report types over different artifact scopes | Preserve both; link by dataset/artifact scope. |
| AB=CD outcome reports | Outcome, repaired outcome, distance outcome, sanity audit | Related sequence, not simple duplicates | Migrate as experiment chain with blocked/debt statuses. |
| Pole validation summaries | BNB/BTC/ETH/SOL/XRP summaries | Same template per symbol | Migrate as symbol-level validation artifacts under one final validation experiment. |
| Settings files | `settings.json`, research clean, Binance demo | Different operational contexts | Preserve; mark research clean as authoritative for baseline validation context. |

### Superseded Documents

| Superseded / lower-authority | Successor / higher-authority | Reason |
| --- | --- | --- |
| `research_v2/patterns/pnf_abcd_symmetry_audit_design.md` | `research_v2/patterns/pnf_abcd_symmetry_audit_design_v2.md` | v2 explicitly revises initial design to close mandatory gaps. |
| BTC-only follow-through investigation/validation | Final validation PR portfolio reality artifacts | BTC-only reports are narrower and earlier than portfolio-level validation. |
| Local harmonic v2 reports for some questions | BTC/ETH/SOL audit reports | Audit bundle appears broader for BTC/ETH/SOL threshold analysis; local v2 remains useful as historical artifact. |
| Demo positive-expectancy settings | Research-clean settings for validation | Demo config is not baseline validation authority. |

### Conflicting Documents

| Potential conflict | Assessment | Recommendation |
| --- | --- | --- |
| Historical experiment docs vs current AGENTS baseline metrics | Possible drift if older reports contain different assumptions. | Treat `AGENTS.md`, acceptance rules, and final validation artifacts as current authority; migrate older reports with historical status. |
| Blocked AB=CD reports vs AB=CD summary/design optimism | Not a true conflict; reports document missing trusted inputs. | Preserve blocked status as research debt. |
| Execution/live docs vs research-only docs | Different scopes, not conflict. | Separate ExecutionWorkflow/Runbook from research validation during migration. |

### Orphan / Low-Traceability Documents

| Type | Documents | Issue | Migration action |
| --- | --- | --- | --- |
| No incoming references | Most standalone reports, including many harmonic/ABCD reports | Normal for reports, but weak traceability | Attach each to experiment/hypothesis IDs during ROS migration. |
| No outgoing references | Many CSVs and summaries | Data artifacts do not describe provenance alone | Pair with companion report or manifest. |
| Reports lacking explicit hypothesis | Several harmonic ratio and AB=CD artifact reports | Descriptive audits may not state hypothesis IDs | Create Hypothesis or ExploratoryAudit records during migration. |
| Hypotheses/designs lacking validation follow-up | Pole genetic miner design, some structure/harmonic designs | Design may not have implementation or validation successor | Mark as `HYPOTHESIS_DEFINED`, `DEFER`, or `RESEARCH_DEBT`. |
| RFC/design lacking implementation follow-up | AB=CD v2 design, harmonic framework/extraction, structural swing aggregation | Design-only by scope | Do not implement; migrate as design records with status. |
| Implementation/config lacking RFC | Some settings and strategy CSV exports | Operational artifacts lack linked rationale | Link to final validation or MVP docs where applicable. |

### Reports That Never Clearly Produced Follow-Up

- `docs/p2_causal_motif_research.md`
- `research_v2/patterns/pole_genetic_hypothesis_miner_design.md`
- `research_v2/structure_validation/pnf_structural_swing_aggregation_design.md`
- `research_v2/patterns/pnf_harmonic_swing_framework_design.md`
- `research_v2/patterns/pnf_harmonic_swing_extraction_design.md`
- Some harmonic ratio stability outputs
- Some AB=CD blocked outcome/population reports

These should not be discarded. They should become hypotheses, failures, exploratory reports, or research debt records depending on the migration evidence.

## Repository Health Estimate

| Dimension | Rating | Notes |
| --- | --- | --- |
| Documentation completeness | Medium-High | Many docs and artifacts exist, but not yet normalized into registries/manifests. |
| Knowledge fragmentation | High | Knowledge is spread across root docs, `docs/`, `experiments/`, `pnf_mvp/exports/`, and `research_v2/`. |
| Duplicate knowledge | Medium | Similar report templates and repeated harmonic/ABCD outputs exist, but most reflect different scopes. |
| Migration readiness | Medium-High | Enough evidence exists to start ROS-0; missing registries/manifests remain. |
| Missing registries | High | No formal hypothesis, edge, failure, promotion, or debt registries yet. |
| Missing manifests | High | Some manifests exist, but most outputs lack standardized ROS manifests. |
| Missing traceability | Medium-High | References exist informally; many reports lack incoming links or explicit hypothesis IDs. |
| Missing confidence history | High | Confidence evolution is not recorded as a formal history. |
| Missing rollback references | Medium | Baseline rollback is documented in governance and logs, but needs first-class migration. |
| Missing ownership | High | Owners are generally not declared. |

## Overall Repository Health

The repository is healthy as a research archive but not yet healthy as a ROS-managed knowledge base. The strongest assets are the governance baseline, final Pole validation artifacts, experiment logs, and rich harmonic/ABCD research outputs. The main weakness is not lack of knowledge; it is fragmented provenance, missing ownership, missing registry IDs, and inconsistent links between hypotheses, validation runs, reports, failures, and promotion decisions.

## Top 20 Highest-Value Documents for ROS Migration

| Rank | Document | Why it is high value |
| ---: | --- | --- |
| 1 | `AGENTS.md` | Defines governance, protected interfaces, stable rollback profile, and required experiment scorecard. |
| 2 | `experiments/experiment_log.md` | Contains stable baseline reference and experiment history; should seed Experiment and Rollback records. |
| 3 | `docs/research_results_log.md` | Versioned strategy/validation snapshots and assumptions; central validation history input. |
| 4 | `pnf_mvp/exports/final_validation_pr_20260701/portfolio_reality_from_raw/final_validation_report.md` | Core final validation evidence for baseline/promotion context. |
| 5 | `pnf_mvp/exports/final_validation_pr_20260701/portfolio_reality_from_raw/portfolio_reality_summary.md` | Concise portfolio reality summary; high-value baseline scorecard artifact. |
| 6 | `pnf_mvp/exports/final_validation_pr_20260701/portfolio_reality_from_raw/portfolio_reality_manifest.json` | Existing manifest-like artifact; useful bridge to ROS manifests. |
| 7 | `experiments/acceptance_rules.md` | Defines baseline-first policy and scorecard expectations. |
| 8 | `README_project_workflow.md` | Captures research-lab operating model and pipeline intent. |
| 9 | `pnf_mvp/exports/research_closure_continuation_failure/research_closure.md` | Preserves negative/closure knowledge for continuation failure. |
| 10 | `pnf_mvp/exports/research_synthesis_continuation_failure/research_synthesis.md` | Synthesizes continuation failure findings and should seed Failure Registry records. |
| 11 | `docs/p2_causal_motif_research.md` | Important Pole/SL-C motif audit with likely hypothesis/failure implications. |
| 12 | `research_v2/README.md` | Explains why isolated research path exists; key migration context. |
| 13 | `research_v2/filters/README.md` | Documents research-only baseline filter profile. |
| 14 | `research_v2/setup_dataset/README.md` | Defines frozen setup dataset export path; essential for Dataset Manifest migration. |
| 15 | `research_v2/labeling/README.md` | Defines deterministic labeling engine; essential for ValidationRun and Feature migration. |
| 16 | `research_v2/patterns/ABCD_MODEL_C_RESEARCH_SUMMARY.md` | Highest-level AB=CD research summary. |
| 17 | `research_v2/patterns/pnf_abcd_symmetry_audit_design_v2.md` | Current AB=CD design authority; supersedes initial design. |
| 18 | `research_v2/patterns/pnf_harmonic_swing_model_comparison.md` | Clarifies harmonic swing model choices. |
| 19 | `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/harmonic_time_stability_report.md` | High-value harmonic stability evidence. |
| 20 | `research_v2/execution/README.md` | Captures execution/MEXC context and links safe live config example. |

## Recommended Migration Order

Do not migrate anything during pre-flight. When migration begins, the recommended order is:

1. Governance and rollback seed: `AGENTS.md`, `experiments/acceptance_rules.md`, `README_project_workflow.md`.
2. Baseline validation seed: final validation report, portfolio reality summary, portfolio reality manifest, symbol pole summaries, key final validation CSVs.
3. Experiment and validation logs: `experiments/experiment_log.md`, `docs/research_results_log.md`, BTC follow-through reports.
4. Research infrastructure: `research_v2/README.md`, setup dataset, labeling, analytics, filters, data README.
5. Failure preservation: continuation failure closure and synthesis, then blocked AB=CD reports.
6. Pole / motif exploratory research: P+2 causal motif, Pole genetic miner design and review.
7. Harmonic research chain: framework/extraction/model comparison, local v2 reports, BTC/ETH/SOL audit reports and companion CSV bundles.
8. ABCD research chain: v2 design, Model C summary, geometry diagnostic, population/outcome/distance/sanity reports and companion tables.
9. Execution/live context: execution README, MEXC config example, replay summary, trade snapshot integration, MVP README/settings.
10. ROS architecture docs: Blueprint, Advanced, Knowledge Migration Review, Decision Engine remain current architecture references and should be linked as migration policy, not migrated as research evidence.

## Appendix A: Exact Committed Artifact Ledger

This ledger lists every committed `*.md`, `*.txt`, `*.json`, and `*.csv` artifact inspected. Detailed dependencies and references should be finalized during ROS-0; where no explicit owner/reference was detected, owner is `Unknown` and references are `None detected` or `companion bundle`.

| File | Category | Purpose / family | Status | Owner | Dependencies | References | Referenced by | Active/historical | Authority |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `AGENTS.md` | Governance | General/Other | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | High/Very High |
| `README_project_workflow.md` | README/Infrastructure | General/Other | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `RESEARCH_DECISION_ENGINE.md` | Document | ROS/Migration | Current architecture/audit | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `RESEARCH_OPERATING_SYSTEM_ADVANCED.md` | Document | ROS/Migration | Current architecture/audit | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `RESEARCH_OPERATING_SYSTEM_BLUEPRINT.md` | Document | ROS/Migration | Current architecture/audit | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `ROS_KNOWLEDGE_MIGRATION_REVIEW.md` | Document | ROS/Migration | Current architecture/audit | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `data/research/README.md` | README/Infrastructure | General/Other | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `docs/PNF_PLATFORM_CAPABILITY_AUDIT.md` | Report/Validation | General/Other | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `docs/PNF_SAAS_PLATFORM_ARCHITECTURE.md` | Architecture/Design | General/Other | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `docs/incremental_structure_engine_design.md` | Architecture/Design | Structure | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `docs/p2_causal_motif_research.md` | Document | Pole/Continuation | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `docs/pnf_research_checkpoint.md` | Document | General/Other | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `docs/research_results_log.md` | Document | General/Other | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `experiments/acceptance_rules.md` | Governance | Strategy Validation | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | High/Very High |
| `experiments/btc_followthrough_investigation.md` | Document | Strategy Validation | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `experiments/btc_pole_followthrough_validation.md` | Report/Validation | Pole/Continuation | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `experiments/experiment_log.md` | Document | Strategy Validation | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `experiments/structural_pattern_outcome_summary_v1.md` | Report/Validation | Strategy Validation | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `mexc_pole_live_config.example.json` | JSON config/manifest | Pole/Continuation | Artifact/config | Unknown | Repository context | None detected | Companion reports/READMEs where applicable | Active or historical; verify | Medium unless superseded |
| `pnf_mvp/README.md` | README/Infrastructure | General/Other | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `pnf_mvp/README_BE_EXPERIMENT.txt` | README/Infrastructure | Strategy Validation | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `pnf_mvp/TRADE_SNAPSHOT_INTEGRATION.md` | Document | Harmonic | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `pnf_mvp/exports/entry_distance_decay_v1.csv` | CSV artifact | General/Other | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `pnf_mvp/exports/entry_distance_decay_v1.md` | Document | General/Other | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Historical artifact | Medium unless superseded |
| `pnf_mvp/exports/final_validation_pr_20260701/columns/BNB_columns.csv` | CSV artifact | Pole/Continuation | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | High/Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/columns/BTC_columns.csv` | CSV artifact | Pole/Continuation | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | High/Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/columns/ETH_columns.csv` | CSV artifact | Pole/Continuation | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | High/Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/columns/SOL_columns.csv` | CSV artifact | Pole/Continuation | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | High/Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/columns/XRP_columns.csv` | CSV artifact | Pole/Continuation | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | High/Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/poles/BNB/pole_patterns.csv` | CSV artifact | Pole/Continuation | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | High/Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/poles/BNB/pole_summary.md` | Report/Validation | Pole/Continuation | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Historical artifact | High/Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/poles/BTC/pole_patterns.csv` | CSV artifact | Pole/Continuation | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | High/Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/poles/BTC/pole_summary.md` | Report/Validation | Pole/Continuation | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Historical artifact | High/Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/poles/ETH/pole_patterns.csv` | CSV artifact | Pole/Continuation | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | High/Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/poles/ETH/pole_summary.md` | Report/Validation | Pole/Continuation | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Historical artifact | High/Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/poles/SOL/pole_patterns.csv` | CSV artifact | Pole/Continuation | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | High/Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/poles/SOL/pole_summary.md` | Report/Validation | Pole/Continuation | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Historical artifact | High/Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/poles/XRP/pole_patterns.csv` | CSV artifact | Pole/Continuation | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | High/Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/poles/XRP/pole_summary.md` | Report/Validation | Pole/Continuation | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Historical artifact | High/Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/portfolio_reality_from_raw/equity_curve_usdt.csv` | CSV artifact | Pole/Continuation | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | High/Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/portfolio_reality_from_raw/final_validation_report.md` | Report/Validation | Pole/Continuation | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Historical artifact | High/Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/portfolio_reality_from_raw/monthly_returns_usdt.csv` | CSV artifact | Pole/Continuation | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | High/Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/portfolio_reality_from_raw/portfolio_reality_equity_curve.csv` | CSV artifact | Pole/Continuation | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | High/Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/portfolio_reality_from_raw/portfolio_reality_flags.csv` | CSV artifact | Pole/Continuation | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | High/Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/portfolio_reality_from_raw/portfolio_reality_manifest.json` | JSON config/manifest | Pole/Continuation | Artifact/config | Unknown | Repository context | None detected | Companion reports/READMEs where applicable | Historical artifact | High/Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/portfolio_reality_from_raw/portfolio_reality_monthly.csv` | CSV artifact | Pole/Continuation | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | High/Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/portfolio_reality_from_raw/portfolio_reality_quarterly.csv` | CSV artifact | Pole/Continuation | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | High/Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/portfolio_reality_from_raw/portfolio_reality_summary.md` | Report/Validation | Pole/Continuation | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Historical artifact | High/Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/portfolio_reality_from_raw/portfolio_reality_symbol_contribution.csv` | CSV artifact | Pole/Continuation | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | High/Very High |
| `pnf_mvp/exports/final_validation_pr_20260701/portfolio_reality_from_raw/portfolio_reality_trade_sequence.csv` | CSV artifact | Pole/Continuation | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | High/Very High |
| `pnf_mvp/exports/research_closure_continuation_failure/research_closure.md` | Document | Pole/Continuation | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Historical artifact | Medium unless superseded |
| `pnf_mvp/exports/research_synthesis_continuation_failure/research_synthesis.md` | Document | Pole/Continuation | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Historical artifact | Medium unless superseded |
| `pnf_mvp/settings.binance_demo_positive_expectancy.json` | JSON config/manifest | Execution/Live | Artifact/config | Unknown | Repository context | None detected | Companion reports/READMEs where applicable | Active or historical; verify | Medium unless superseded |
| `pnf_mvp/settings.json` | JSON config/manifest | Execution/Live | Artifact/config | Unknown | Repository context | None detected | Companion reports/READMEs where applicable | Active or historical; verify | Medium unless superseded |
| `pnf_mvp/settings.research_clean.json` | JSON config/manifest | Execution/Live | Artifact/config | Unknown | Repository context | None detected | Companion reports/READMEs where applicable | Active or historical; verify | Medium unless superseded |
| `pnf_mvp/strategy_diagnostics_breakdowns.csv` | CSV artifact | Strategy Validation | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `pnf_mvp/strategy_stopped_review.csv` | CSV artifact | Strategy Validation | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `pnf_mvp/strategy_tp2_review.csv` | CSV artifact | Pole/Continuation | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `pnf_mvp/strategy_trades_export.csv` | CSV artifact | Strategy Validation | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `replay_summary.md` | Report/Validation | Execution/Live | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/README.md` | README/Infrastructure | General/Other | Current/successor where applicable | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/analytics/README.md` | README/Infrastructure | General/Other | Current/successor where applicable | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/execution/README.md` | README/Infrastructure | Execution/Live | Current/successor where applicable | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/filters/README.md` | README/Infrastructure | General/Other | Current/successor where applicable | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/labeling/README.md` | README/Infrastructure | General/Other | Current/successor where applicable | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/patterns/ABCD_MODEL_C_RESEARCH_SUMMARY.md` | Report/Validation | ABCD | Current/successor where applicable | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/patterns/abcd_geometry_input_diagnostic.md` | Document | ABCD | Current/successor where applicable | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/patterns/abcd_outcome_distance_local_v1/abcd_outcome_distance_by_symbol.csv` | CSV artifact | ABCD | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/abcd_outcome_distance_local_v1/abcd_outcome_distance_by_year.csv` | CSV artifact | ABCD | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/abcd_outcome_distance_local_v1/abcd_outcome_distance_report.md` | Report/Validation | ABCD | Current/successor where applicable | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/patterns/abcd_outcome_distance_local_v1/abcd_outcome_distance_sample.csv` | CSV artifact | ABCD | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/abcd_outcome_distance_local_v1/abcd_outcome_distance_summary.csv` | CSV artifact | ABCD | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/abcd_outcome_local_v1/abcd_outcome_by_symbol.csv` | CSV artifact | ABCD | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/abcd_outcome_local_v1/abcd_outcome_by_year.csv` | CSV artifact | ABCD | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/abcd_outcome_local_v1/abcd_outcome_report.md` | Report/Validation | ABCD | BLOCKED/historical | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/patterns/abcd_outcome_local_v1/abcd_outcome_summary.csv` | CSV artifact | ABCD | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/abcd_outcome_repaired_local_v1/abcd_outcome_repaired_by_symbol.csv` | CSV artifact | ABCD | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/abcd_outcome_repaired_local_v1/abcd_outcome_repaired_by_year.csv` | CSV artifact | ABCD | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/abcd_outcome_repaired_local_v1/abcd_outcome_repaired_report.md` | Report/Validation | ABCD | BLOCKED/historical | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/patterns/abcd_outcome_repaired_local_v1/abcd_outcome_repaired_summary.csv` | CSV artifact | ABCD | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/abcd_outcome_sanity_audit_local_v1/outcome_classification_validation.csv` | CSV artifact | ABCD | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/abcd_outcome_sanity_audit_local_v1/outcome_direction_flow_report.md` | Report/Validation | ABCD | Current/successor where applicable | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/patterns/abcd_outcome_sanity_audit_local_v1/outcome_direction_trace_sample.csv` | CSV artifact | ABCD | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/abcd_population_local_v1/abcd_population_by_symbol.csv` | CSV artifact | ABCD | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/abcd_population_local_v1/abcd_population_by_year.csv` | CSV artifact | ABCD | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/abcd_population_local_v1/abcd_population_report.md` | Report/Validation | ABCD | BLOCKED/historical | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/patterns/abcd_population_local_v1/abcd_population_summary.csv` | CSV artifact | ABCD | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/harmonic_cross_period_consistency.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/harmonic_knowledge_time_summary.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/harmonic_leg_statistics.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/harmonic_level_rankings.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/harmonic_level_survival.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/harmonic_period_comparison.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/harmonic_pivot_counts.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/harmonic_ratio_cluster_strength.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/harmonic_ratio_distribution.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/harmonic_ratio_nearest_level.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/harmonic_ratio_summary.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/harmonic_ratio_symbol_breakdown.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/harmonic_reaction_ratio_distribution.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/harmonic_reactions_by_threshold.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/harmonic_swings_by_threshold.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/harmonic_threshold_summary.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/harmonic_time_stability_report.md` | Report/Validation | Harmonic | Current/successor where applicable | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/harmonic_time_stability_summary.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/ratio_predictive_by_symbol.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/ratio_predictive_by_year.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/ratio_predictive_report.md` | Report/Validation | Harmonic | Current/successor where applicable | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/ratio_predictive_stability_report.md` | Report/Validation | Harmonic | Current/successor where applicable | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/ratio_predictive_summary.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/raw_ratio_density_report.md` | Report/Validation | Harmonic | Current/successor where applicable | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/raw_ratio_histogram.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/raw_ratio_histogram_2024.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/raw_ratio_histogram_2025.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/raw_ratio_histogram_2026.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/raw_ratio_peak_stability.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/audit/raw_ratio_peaks.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/columns/BTCUSDT_columns.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/columns/ETHUSDT_columns.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_audit_btc_eth_sol/columns/SOLUSDT_columns.csv` | CSV artifact | Harmonic | Current/successor where applicable | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_local_v2/harmonic_box_size_manifest.csv` | CSV artifact | Harmonic | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_local_v2/harmonic_knowledge_time_summary.csv` | CSV artifact | Harmonic | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_local_v2/harmonic_leg_statistics.csv` | CSV artifact | Harmonic | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_local_v2/harmonic_pivot_counts.csv` | CSV artifact | Harmonic | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_local_v2/harmonic_reaction_ratio_distribution.csv` | CSV artifact | Harmonic | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_local_v2/harmonic_reactions_by_threshold.csv` | CSV artifact | Harmonic | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_local_v2/harmonic_swings_by_threshold.csv` | CSV artifact | Harmonic | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_local_v2/harmonic_threshold_summary.csv` | CSV artifact | Harmonic | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_local_v2/ratio_predictive_by_symbol.csv` | CSV artifact | Harmonic | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_local_v2/ratio_predictive_by_year.csv` | CSV artifact | Harmonic | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/harmonic_swing_threshold_local_v2/ratio_predictive_report.md` | Report/Validation | Harmonic | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/patterns/harmonic_swing_threshold_local_v2/ratio_predictive_stability_report.md` | Report/Validation | Harmonic | Active or historical; verify during migration | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/patterns/harmonic_swing_threshold_local_v2/ratio_predictive_summary.csv` | CSV artifact | Harmonic | Artifact/config | Unknown | Companion report/artifact bundle | No outgoing; companion data | Companion reports/READMEs where applicable | Historical artifact | Companion-dependent |
| `research_v2/patterns/pnf_abcd_symmetry_audit_design.md` | Architecture/Design | ABCD | Current/successor where applicable | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/patterns/pnf_abcd_symmetry_audit_design_v2.md` | Architecture/Design | ABCD | Current/successor where applicable | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/patterns/pnf_harmonic_swing_extraction_design.md` | Architecture/Design | Harmonic | Current/successor where applicable | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/patterns/pnf_harmonic_swing_framework_design.md` | Architecture/Design | Harmonic | Current/successor where applicable | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/patterns/pnf_harmonic_swing_model_comparison.md` | Document | Harmonic | Current/successor where applicable | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/patterns/pole_genetic_hypothesis_miner_design.md` | Architecture/Design | Pole/Continuation | Current/successor where applicable | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/patterns/pole_genetic_hypothesis_miner_review.md` | Document | Pole/Continuation | Current/successor where applicable | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/setup_dataset/README.md` | README/Infrastructure | General/Other | Current/successor where applicable | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
| `research_v2/structure_validation/pnf_structural_swing_aggregation_design.md` | Architecture/Design | Structure | Current/successor where applicable | Unknown | Repository context | None detected | See cross-reference analysis; often none detected | Active or historical; verify | Medium unless superseded |
