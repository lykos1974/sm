# Architecture Freeze V1

## Purpose

This document freezes the Research Operating System architecture to prevent further abstraction drift.

The next project phase is practical migration and real research, not architecture expansion.

## Frozen Core Architecture

The Research Operating System core architecture is now:

```text
Reality
↓
Evidence
↓
Hypothesis
↓
Validation
↓
Decision
↓
Knowledge
```

Everything else must be treated as metadata unless a real migration or real research use case proves otherwise.

## Freeze Rules

- No new architecture layers.
- No new registries unless a real migration or use case proves existing objects cannot represent the information.
- No new subsystem should be created for theoretical completeness.
- No architecture expansion should occur before practical migration and real research expose a concrete representational failure.
- Architecture additions must remain minimal, reversible, and compatible with the protected Pole baseline.

## Causal Memory Placement

Causal memory is **not** a separate architecture layer.

Causal memory belongs inside `Knowledge` metadata, including:

- `causal_history`
- `belief_history`
- `confidence_history`
- `assumptions`
- `contradictions`
- `reactivation_conditions`
- `dormant_reason`
- `rollback_history`
- `promotion_history`
- `research_debt`

The system must remember why knowledge changed, but that memory is metadata attached to Knowledge rather than a new layer above or beside Knowledge.

## Statuses Are Not Layers

The following are statuses or state descriptors, not architecture layers:

- `dormant`
- `rejected`
- `superseded`
- `promoted`
- `waiting_for_data`
- `waiting_for_larger_sample`

These states may describe Evidence, Hypotheses, Validations, Decisions, or Knowledge, but they must not create new architecture layers or registries by default.

## Counterfactuals And Future Recommendations

Counterfactual knowledge is `reactivation_conditions` metadata, not a new subsystem.

Future recommendations are metadata, not a new registry.

Examples of future recommendations that should remain metadata include:

- revisit after detector v2;
- revisit when liquidation data exists;
- revisit after OI feed;
- revisit after volatility model;
- revisit after larger sample;
- revisit after higher timeframe data;
- revisit after market regime change.

## Strict New Concept Gate

A new concept may be added only if **all** of the following are true:

1. It cannot be represented as metadata of Evidence, Hypothesis, Validation, Decision, or Knowledge.
2. It reduces wasted research time.
3. It improves reproducibility or decision quality.
4. It does not threaten the protected Pole baseline.
5. It has a real project use case, not theoretical appeal.

If any condition is false, the concept must not be added as a new architecture concept, layer, registry, subsystem, or abstraction.

## Protected Baseline Constraint

The architecture freeze must not modify, reinterpret, or weaken the protected Pole baseline.

Registry and documentation work must remain separate from detector logic, strategy logic, validation logic, execution logic, datasets, exports, sqlite files, parquet files, and research code.

## Next Phase

After this file, the next project phase is practical migration and real research.

The Research Operating System should now be tested by migrating real families and recording real evidence, hypotheses, validations, decisions, and knowledge using the frozen architecture.

Architecture should expand only if the Strict New Concept Gate is satisfied by a real project use case.

## Final Verdict

ARCHITECTURE_FREEZE_V1_ACTIVE
